package types

import (
	"time"

	"k8s.io/client-go/kubernetes"
)

type PodInfo struct {
	Name         string
	Namespace    string
	CreationTime time.Time
	Duration     time.Duration
}

type Resource_generator struct {
	FileName   string
	KubeClient *kubernetes.Clientset
}
