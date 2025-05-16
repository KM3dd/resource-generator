package resource_generator

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
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

// watchFile monitors the specified file for pod creation and deletion
func (r *Resource_generator) WatchFile() {
	for {
		file, err := os.Open(r.FileName)
		if err != nil {
			log.Printf("Error opening file: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			podInfo, err := parsePodInfo(line)
			if err != nil {
				log.Printf("Error parsing pod info: %v", err)
				continue
			}

			// Create pod at specified creation time
			go func(pod types.PodInfo) {
				log.Printf("Pod creation time is : %v", pod.CreationTime)
				time.Sleep(time.Until(pod.CreationTime))
				err := k8s_manager.CreatePod(r.KubeClient, pod)
				if err != nil {
					log.Printf("Error creating pod %s: %v", pod.Name, err)
				}
			}(podInfo)

			// Delete pod at specified deletion time
			go func(pod types.PodInfo) {
				time.Sleep(time.Until(pod.DeletionTime))
				err := k8s_manager.DeletePod(r.KubeClient, pod)
				if err != nil {
					log.Printf("Error deleting pod %s: %v", pod.Name, err)
				}
			}(podInfo)
		}

		if err := scanner.Err(); err != nil {
			log.Printf("Error reading file: %v", err)
		}

		// Wait before checking the file again
		time.Sleep(5 * time.Second)
	}
}

// parsePodInfo parses a line from the file into PodInfo
// Expected format: PodName,Namespace,CreationTimestamp,DeletionTimestamp
func parsePodInfo(line string) (types.PodInfo, error) {
	parts := strings.Split(line, ",")
	if len(parts) != 4 {
		return types.PodInfo{}, fmt.Errorf("invalid pod info format")
	}

	creationTime, err := time.Parse(time.RFC3339, parts[2])
	if err != nil {
		return types.PodInfo{}, fmt.Errorf("invalid creation time: %v", err)
	}

	deletionTime, err := time.Parse(time.RFC3339, parts[3])
	if err != nil {
		return types.PodInfo{}, fmt.Errorf("invalid deletion time: %v", err)
	}

	return types.PodInfo{
		Name:         parts[0],
		Namespace:    parts[1],
		CreationTime: creationTime,
		DeletionTime: deletionTime,
	}, nil
}
