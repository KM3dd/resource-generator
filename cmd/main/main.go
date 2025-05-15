package main

import (
	"log"
	"os"

	resource_generator "github.com/KM3dd/resource-generator/internal"
	"github.com/KM3dd/resource-generator/internal/k8s_manager"
)

func main() {

	kubeconfig, _ := k8s_manager.CreateKubernetesClient()

	filepath := os.Getenv("HOME") + "{user}/resource-generator/pod_data.txt"
	r, _ := resource_generator.NewResourceGenerator(filepath, kubeconfig)

	log.Printf("Starting resource generation...")

	r.WatchFile()

}
