package k8s_manager

import (
	context "context"
	"fmt"
	"os"
	"path/filepath"

	types "github.com/KM3dd/resource-generator/internal/types"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
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
func CreatePod(clientset *kubernetes.Clientset, podInfo types.PodInfo) error {

	job := &batchv1.Job{
		ObjectMeta: metav1meta.ObjectMeta{
			Name: podInfo.Name,
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{
						{
							Name:    "gpu-job",
							Image:   "nvidia/cuda:12.2.0-base-ubuntu22.04", // Example CUDA image
							Command: []string{"nvidia-smi"},                // Simple GPU command
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									"nvidia.com/mig-1g.5gb": resource.MustParse("1"),
								},
								Limits: corev1.ResourceList{
									"nvidia.com/mig-1g.5gb": resource.MustParse("1"),
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
func DeletePod(clientset *kubernetes.Clientset, podInfo types.PodInfo) error {
	return clientset.CoreV1().Pods(podInfo.Namespace).Delete(
		context.Background(),
		podInfo.Name,
		metav1.DeleteOptions{},
	)
}
