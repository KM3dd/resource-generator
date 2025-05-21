package k8s_manager

import (
	context "context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	types "github.com/KM3dd/resource-generator/internal/types"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// createKubernetesClient creates a Kubernetes client
func CreateKubernetesClient() (*kubernetes.Clientset, error) {
	// Try to load from kubeconfig first
	kubeconfig := filepath.Join(os.Getenv("HOME"), ".kube", "config")
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)

	if err != nil {
		fmt.Errorf("Failed to load kubeconfig: %v", err)
	}
	// If kubeconfig fails, try in-cluster configuration
	if err != nil {
		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("error loading Kubernetes configuration: %v", err)
		}
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		fmt.Errorf("Failed to create clientset: %v the clientset %v", err, clientset)
	}

	// Create the clientset
	return kubernetes.NewForConfig(config)
}

// createPod creates a Kubernetes pod
func CreateJob(clientset *kubernetes.Clientset, podInfo types.PodInfo) error {

	resourceName := fmt.Sprintf("nvidia.com/mig-%s", podInfo.Resource)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: podInfo.Name,
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{
						{
							Name:    podInfo.Name,
							Image:   "nvidia/cuda:12.2.0-base-ubuntu22.04",
							Command: []string{"nvidia-smi"},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceName(resourceName): resource.MustParse("1"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceName(resourceName): resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
		},
	}

	_, err := clientset.BatchV1().Jobs("default").Create(context.TODO(), job, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create job: %w", err)
	}
	return nil
}

// deletePod deletes a Kubernetes pod
func DeleteJob(clientset *kubernetes.Clientset, podInfo types.PodInfo) error {

	deletePolicy := metav1.DeletePropagationForeground
	err := clientset.BatchV1().Jobs("default").Delete(
		context.Background(),
		podInfo.Name,
		metav1.DeleteOptions{
			PropagationPolicy: &deletePolicy,
		},
	)
	return err
}

// watches until pod is ungated
func WatchUntilUngated(clientset *kubernetes.Clientset, podInfo types.PodInfo) error {

	log.Printf("Starting to check if pod is gated : %v", podInfo.Name)
	pod, err := clientset.CoreV1().Pods("").Get(context.TODO(), podInfo.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error getting pod details: %v", err)
	}

	// If pod is already ungated, return immediately
	if !checkIfPodGatedByInstaSlice(pod) {
		log.Printf("Pod %s is already ungated, no need to watch ", podInfo.Name)
		return nil
	}

	// Else : setup the watcher
	watcher, err := clientset.CoreV1().Pods("default").Watch(context.TODO(), metav1.ListOptions{
		FieldSelector: fmt.Sprintf("metadata.name=%s", podInfo.Name),
	})

	if err != nil {
		return fmt.Errorf("Something went wront when trying to setup watcher %v", err)
	}

	defer watcher.Stop()

	for {
		select {
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return fmt.Errorf("watch channel closed unexpectedly")
			}

			pod, ok := event.Object.(*v1.Pod)
			if !ok {
				continue // Skip if the object is not a pod
			}

			// Check if the pod is ungated
			if !checkIfPodGatedByInstaSlice(pod) {
				log.Printf("Pod %s is now ungated", podInfo.Name)
				return nil
			}
		}
	}

	//return false
}

//func isUngated(pod *v1.Pod) bool {
//	for _, conditions := range pod.Status.Conditions {
//		if conditions.Type == v1.PodScheduled && conditions.Status == v1.ConditionFalse && conditions.Reason == "Gated" {
//			return false
//		}
//	}
//}

func checkIfPodGatedByInstaSlice(pod *v1.Pod) bool {
	fmt.Printf("gates %v", pod.Spec.SchedulingGates)

	if pod.Status.Conditions[0].Reason == v1.PodReasonSchedulingGated {
		return true
	}

	return false
}
