package k8s_manager

import (
	context "context"
	"fmt"
	"os"
	"path/filepath"

	types "github.com/KM3dd/resource-generator/internal/types"
	corev1 "k8s.io/api/core/v1"
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

	//debug trying to list ...
	podList, listErr := clientset.CoreV1().Pods(podInfo.Namespace).List(context.Background(), metav1.ListOptions{})
	if listErr != nil {
		fmt.Printf("Warning: Cannot list pods in namespace %s: %v\n", podInfo.Namespace, listErr)
	} else {
		fmt.Printf("Successfully listed %d pods in namespace %s\n", len(podList.Items), podInfo.Namespace)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podInfo.Name,
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  podInfo.Name,
					Image: "nginx",
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
