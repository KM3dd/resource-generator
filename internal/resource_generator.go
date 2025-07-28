package resource_generator

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/KM3dd/resource-generator/internal/k8s_manager"
	types "github.com/KM3dd/resource-generator/internal/types"
	"k8s.io/client-go/kubernetes"
)

type Resource_generator types.Resource_generator

var fileMutex sync.Mutex

func NewResourceGenerator(
	FileName string,
	kubeClient *kubernetes.Clientset,
) (*Resource_generator, error) {

	r := &Resource_generator{
		FileName:   FileName,
		KubeClient: kubeClient,
	}

	return r, nil
}

func (r *Resource_generator) Generate() error {
	podInfos, err := readPodConfigFile(r.FileName)
	if err != nil {
		log.Fatalf("Error reading pod config file: %v", err)
	}
	log.Printf("Read %d pod configurations from %s", len(podInfos), r.FileName)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	go handleShutdown(cancel)

	// Schedule pod creation and deletion
	var wg sync.WaitGroup
	for _, podInfo := range podInfos {
		wg.Add(1)
		go func(info types.PodInfo) {
			managePod(ctx, r.KubeClient, info)
			wg.Done()
		}(podInfo)
	}
	wg.Wait()

	log.Printf("Generation done ... writing overall results to results.json")

	// Calculate metrics including memory-based waiting times
	fileMutex.Lock()
	defer fileMutex.Unlock()

	file, err := os.Open("results.json")
	if err != nil {
		return err
	}
	defer file.Close()

	var totalWaitMs int64
	var minWaitMs int64 = -1
	var maxWaitMs int64
	var count int64

	// Profile-based metrics (full profile name like "4g.20gb")
	profileMetrics := make(map[string]struct {
		totalWait int64
		count     int64
		minWait   int64
		maxWait   int64
	})

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry types.WaitTimeRecord
		err := json.Unmarshal(scanner.Bytes(), &entry)
		if err != nil {
			fmt.Println("Error parsing line:", err)
			continue
		}

		wait := entry.WaitMs
		totalWaitMs += wait
		if minWaitMs == -1 || wait < minWaitMs {
			minWaitMs = wait
		}
		if wait > maxWaitMs {
			maxWaitMs = wait
		}
		count++

		// Process profile-based metrics (full profile name)
		resourceProfile := strings.ToLower(entry.Resource)

		if profileMetric, exists := profileMetrics[resourceProfile]; exists {
			profileMetric.totalWait += wait
			profileMetric.count++
			if profileMetric.minWait == -1 || wait < profileMetric.minWait {
				profileMetric.minWait = wait
			}
			if wait > profileMetric.maxWait {
				profileMetric.maxWait = wait
			}
			profileMetrics[resourceProfile] = profileMetric
		} else {
			// Create new profile metric entry
			profileMetrics[resourceProfile] = struct {
				totalWait int64
				count     int64
				minWait   int64
				maxWait   int64
			}{
				totalWait: wait,
				count:     1,
				minWait:   wait,
				maxWait:   wait,
			}
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
		return err
	}

	if count == 0 {
		fmt.Println("No entries found.")
		return fmt.Errorf("no entries found")
	}

	avgWaitMs := float64(totalWaitMs) / float64(count)

	// Log overall results
	log.Printf("Total Wait Time: %d ms\n", totalWaitMs)
	log.Printf("Average Wait Time: %.2f ms\n", avgWaitMs)
	log.Printf("Min Wait Time: %d ms\n", minWaitMs)
	log.Printf("Max Wait Time: %d ms\n", maxWaitMs)

	// Write results to text file
	resultsFile, err := os.OpenFile("results.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("error opening results.txt: %v", err)
	}
	defer resultsFile.Close()

	writer := bufio.NewWriter(resultsFile)
	defer writer.Flush()

	// Write timestamp
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	fmt.Fprintf(writer, "\n=== Pod Generation Results - %s ===\n", timestamp)

	// Write overall metrics
	fmt.Fprintf(writer, "Overall Metrics:\n")
	fmt.Fprintf(writer, "  Total Pods: %d\n", count)
	fmt.Fprintf(writer, "  Total Wait Time: %d ms\n", totalWaitMs)
	fmt.Fprintf(writer, "  Average Wait Time: %.2f ms\n", avgWaitMs)
	fmt.Fprintf(writer, "  Min Wait Time: %d ms\n", minWaitMs)
	fmt.Fprintf(writer, "  Max Wait Time: %d ms\n", maxWaitMs)

	// Write profile-based metrics
	fmt.Fprintf(writer, "\nProfile-Based Wait Time Metrics:\n")

	// Sort profiles for consistent output
	profiles := make([]string, 0, len(profileMetrics))
	for profile := range profileMetrics {
		profiles = append(profiles, profile)
	}
	sort.Strings(profiles)

	for _, profile := range profiles {
		metric := profileMetrics[profile]
		avgProfileWait := float64(metric.totalWait) / float64(metric.count)

		fmt.Fprintf(writer, "  Profile '%s':\n", profile)
		fmt.Fprintf(writer, "    Count: %d pods\n", metric.count)
		fmt.Fprintf(writer, "    Average Wait Time: %.2f ms\n", avgProfileWait)
		fmt.Fprintf(writer, "    Min Wait Time: %d ms\n", metric.minWait)
		fmt.Fprintf(writer, "    Max Wait Time: %d ms\n", metric.maxWait)
		fmt.Fprintf(writer, "    Total Wait Time: %d ms\n", metric.totalWait)

		// Log to console as well
		log.Printf("Profile '%s' - Count: %d, Avg Wait: %.2f ms, Min: %d ms, Max: %d ms",
			profile, metric.count, avgProfileWait, metric.minWait, metric.maxWait)
	}

	fmt.Fprintf(writer, "\n"+strings.Repeat("=", 50)+"\n")

	log.Printf("Results written to results.txt")
	return nil
}

// managePod handles creating and deleting a pod based on its schedule
func managePod(ctx context.Context, clientset *kubernetes.Clientset, podInfo types.PodInfo) {

	// Calculate start and end times
	now := time.Now()
	timeOfStart := now.Add(podInfo.CreationTime)
	waitForStart := time.Until(timeOfStart)
	endTime := timeOfStart.Add(podInfo.Duration)
	//waitForEnd := time.Until(endTime)
	podKey := fmt.Sprintf("Starting to manage %s/%s", podInfo.Resource, podInfo.Name)

	// Check if pod should be created at all
	if endTime.Before(now) {
		log.Printf("Pod %s scheduled end time is in the past, skipping", podKey)
		return
	}

	// If start time is in the past but end time is in the future, create now
	if timeOfStart.Before(now) {
		log.Printf("Pod %s scheduled start time is in the past, creating now", podKey)
		waitForStart = 0
	}

	// Wait until start time
	if waitForStart > 0 {
		log.Printf("Waiting %v to create pod %s", waitForStart, podKey)
		select {
		case <-time.After(waitForStart):
			log.Printf("Time to create pod %s", podKey)
		case <-ctx.Done():
			log.Printf("Context cancelled while waiting to create pod %s", podKey)
			return
		}
	}

	// Create pod
	err := k8s_manager.CreatePod(clientset, podInfo)
	if err != nil {
		log.Printf("Error creating pod %s: %v", podKey, err)
		return
	}
	// wait for pod to be ungated befor waiting until end time ...
	waitTimeStart := time.Now()
	k8s_manager.WatchUntilUngated(clientset, podInfo)

	//calculate wait time ..

	WaitTimeEnd := time.Now()
	endTime = WaitTimeEnd.Add(podInfo.Duration)
	// Wait until end time
	endWait := time.Until(endTime)
	log.Printf("Waiting %v to delete pod %s", endWait, podKey)
	select {
	case <-time.After(endWait):
		log.Printf("Time to delete pod %s", podKey)
	case <-ctx.Done():
		log.Printf("Context cancelled while waiting to delete pod %s", podKey)
		return
	}

	// Delete pod
	err = k8s_manager.DeletePod(clientset, podInfo)
	if err != nil {
		log.Printf("Error deleting pod %s: %v", podKey, err)
		return
	}
	log.Printf("Pod %s deleted successfully", podKey)

	waitTime := WaitTimeEnd.Sub(waitTimeStart)
	log.Printf("Storing wait time.. ")

	WriteToFile("results.json", waitTimeStart, WaitTimeEnd, waitTime, podInfo)

}

// readPodConfigFile reads pod configurations from a file
// Format: PodName,Namespace,StartTime,Duration(in minutes)
func readPodConfigFile(filePath string) ([]types.PodInfo, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	var podInfos []types.PodInfo
	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.Split(line, ",")
		if len(parts) != 4 {
			return nil, fmt.Errorf("invalid format at line %d, expected 4 fields but got %d", lineNum, len(parts))
		}

		// Parse start time
		startTime, err := strconv.Atoi(strings.TrimSpace(parts[2]))
		if err != nil {
			return nil, fmt.Errorf("invalid start time at line %d: %v", lineNum, err)
		}

		// Parse duration (in minutes)
		durationSeconds, err := strconv.Atoi(strings.TrimSpace(parts[3]))
		if err != nil {
			return nil, fmt.Errorf("invalid duration at line %d: %v", lineNum, err)
		}

		podInfo := types.PodInfo{
			Name:         strings.TrimSpace(parts[0]),
			Resource:     strings.TrimSpace(parts[1]),
			CreationTime: time.Duration(startTime) * time.Second,
			Duration:     time.Duration(durationSeconds) * time.Second,
		}

		podInfos = append(podInfos, podInfo)
		log.Printf("Read pod config: %s Resource %s, start: %v, duration: %v seconds",
			podInfo.Name, podInfo.Resource, podInfo.CreationTime, durationSeconds)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %v", err)
	}

	return podInfos, nil
}

func WriteToFile(filename string, waitTimeStart time.Time, WaitTimeEnd time.Time, waitTime time.Duration, pod types.PodInfo) error {
	record := types.WaitTimeRecord{
		PodName:   pod.Name,
		Resource:  pod.Resource,
		StartTime: waitTimeStart,
		WaitMs:    waitTime.Milliseconds(),
		EndTime:   WaitTimeEnd,
	}

	fileMutex.Lock()
	defer fileMutex.Unlock()

	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	return encoder.Encode(record)
}

// watchFile monitors the specified file for pod creation and deletion
func (r *Resource_generator) WatchFile() {
	file, err := os.Open(r.FileName)
	if err != nil {
		log.Printf("Error opening file: %v", err)
		time.Sleep(5 * time.Second)

	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		line, _ := reader.ReadString('\n')
		podInfo, err := parsePodInfo(line)
		if err != nil {
			log.Printf("Error parsing pod info: %v", err)
			continue
		}

		// Create pod at specified creation time
		go func(pod types.PodInfo) {
			log.Printf("Pod creation time is : %v", pod.CreationTime)

			err := k8s_manager.CreateJob(r.KubeClient, pod)
			if err != nil {
				log.Printf("Error creating pod %s: %v", pod.Name, err)
			}
		}(podInfo)

		// Delete pod at specified deletion time
		go func(pod types.PodInfo) {
			//			time.Sleep(time.Until(pod.Duration))
			err := k8s_manager.DeleteJob(r.KubeClient, pod)
			if err != nil {
				log.Printf("Error deleting pod %s: %v", pod.Name, err)
			}
		}(podInfo)
	}

	// Wait before checking the file again
	//time.Sleep(5 * time.Second)

}

func handleShutdown(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Println("Received shutdown signal. Cleaning up...")
	cancel()
	os.Exit(0)
}

// parsePodInfo parses a line from the file into PodInfo
// Expected format: PodName,Namespace,CreationTimestamp,DeletionTimestamp
func parsePodInfo(line string) (types.PodInfo, error) {
	parts := strings.Split(line, ",")
	if len(parts) != 4 {
		return types.PodInfo{}, fmt.Errorf("invalid pod info format")
	}

	startTime, err := strconv.Atoi(strings.TrimSpace(parts[2]))
	if err != nil {
		return types.PodInfo{}, fmt.Errorf("invalid creation time: %v", err)
	}

	durationSeconds, err := strconv.Atoi(strings.TrimSpace(parts[3]))
	if err != nil {
		return types.PodInfo{}, fmt.Errorf("invalid deletion time: %v", err)
	}

	return types.PodInfo{
		Name:         parts[0],
		Resource:     parts[1],
		CreationTime: time.Duration(startTime) * time.Second,
		Duration:     time.Duration(durationSeconds) * time.Second,
	}, nil
}
