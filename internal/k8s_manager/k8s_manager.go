package k8s_manager

import (
	context "context"

	types "github.com/KM3dd/resource-generator/internal/types"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// createPod creates a Kubernetes pod
func CreatePod(clientset *kubernetes.Clientset, podInfo types.PodInfo) error {
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
