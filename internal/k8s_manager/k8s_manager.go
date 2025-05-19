package k8s_manager

import (
	context "context"
	"fmt"
	"os"
	"path/filepath"

	types "github.com/KM3dd/resource-generator/internal/types"
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

	// If kubeconfig fails, try in-cluster configuration
	if err != nil {
		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("error loading Kubernetes configuration: %v", err)
		}
	}

	// Create the clientset
	return kubernetes.NewForConfig(config)
}

// createPod creates a Kubernetes pod
func CreatePod(clientset *kubernetes.Clientset, podInfo types.PodInfo) error {
	resourceName := fmt.Sprintf("nvidia.com/mig-%s", podInfo.Resource)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podInfo.Name,
			Namespace: podInfo.Namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  podInfo.Name,
					Image: "nginx",
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceName(resourceName): resource.MustParse("1"),
						},
						Requests: corev1.ResourceList{
							corev1.ResourceName(resourceName): resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	_, err := clientset.CoreV1().Pods(podInfo.Namespace).Create(context.Background(), pod, metav1.CreateOptions{})
	return err
}

// deletePod deletes a Kubernetes pod
func DeletePod(clientset *kubernetes.Clientset, podInfo types.PodInfo) error {
	return clientset.CoreV1().Pods(podInfo.Namespace).Delete(
		context.Background(),
		podInfo.Name,
		metav1.DeleteOptions{},
	)
}
