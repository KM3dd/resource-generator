package resource_generator

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
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

func (r *Resource_generator) Generate() {

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

}

// managePod handles creating and deleting a pod based on its schedule
func managePod(ctx context.Context, clientset *kubernetes.Clientset, podInfo types.PodInfo) {

	// Calculate start and end times
	now := time.Now()
	timeOfStart := now.Add(time.Second * podInfo.CreationTime)
	waitForStart := time.Until(timeOfStart)
	endTime := timeOfStart.Add(podInfo.Duration)
	//waitForEnd := time.Until(endTime)
	podKey := fmt.Sprintf("Starting to manage %s/%s ... wait for start is %v", podInfo.Namespace, podInfo.Name, waitForStart)

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
	log.Printf("Pod %s created successfully", podKey)

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
			Namespace:    strings.TrimSpace(parts[1]),
			CreationTime: time.Duration(startTime) * time.Second,
			Duration:     time.Duration(durationSeconds) * time.Second,
		}

		podInfos = append(podInfos, podInfo)
		log.Printf("Read pod config: %s in namespace %s, start: %v, duration: %v seconds",
			podInfo.Name, podInfo.Namespace, podInfo.CreationTime, durationSeconds)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %v", err)
	}

	return podInfos, nil
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

			err := k8s_manager.CreatePod(r.KubeClient, pod)
			if err != nil {
				log.Printf("Error creating pod %s: %v", pod.Name, err)
			}
		}(podInfo)

		// Delete pod at specified deletion time
		go func(pod types.PodInfo) {
			//			time.Sleep(time.Until(pod.Duration))
			err := k8s_manager.DeletePod(r.KubeClient, pod)
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
		Namespace:    parts[1],
		CreationTime: time.Duration(startTime) * time.Second,
		Duration:     time.Duration(durationSeconds) * time.Second,
	}, nil
}
