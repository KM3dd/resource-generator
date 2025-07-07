package main

import (
	"log"
	"os"

	resource_generator "github.com/KM3dd/resource-generator/internal"
	"github.com/KM3dd/resource-generator/internal/k8s_manager"
)

func main() {

	kubeconfig, _ := k8s_manager.CreateKubernetesClient()

	filename := os.Args[1]
	filepath := os.Getenv("PWD") + "/" + filename
	log.Printf("file path is %v", filepath)
	r, _ := resource_generator.NewResourceGenerator(filepath, kubeconfig)

	log.Printf("Starting resource generation...")

	// launch metric collector

	// go r.metricCollector

	//launching generator
	r.Generate()

}
