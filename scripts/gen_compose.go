package main

import (
	"fmt"
	"os"
	"sort"
	"text/template"

	"gopkg.in/yaml.v3"
)

type WorkerDef struct {
	Name   string `yaml:"name"`
	Amount int    `yaml:"amount"`
}

type Topology struct {
	Clients   int                    `yaml:"clients"`
	Pipelines map[string][]WorkerDef `yaml:"pipelines"`
}

type WorkerInstance struct {
	ContainerName    string
	WorkerPrefix     string
	ID               int
	WorkerAmount     int
	ConfigPath       string
	VolumeMapping    string
	HasNext          bool
	NextWorkerPrefix string
	NextWorkerAmount int
}

type ClientInstance struct {
	ID int
}

type TemplateData struct {
	Clients []ClientInstance
	Workers []WorkerInstance
}

func main() {
	topoData, err := os.ReadFile("topology.yaml")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading topology.yaml: %v\n", err)
		os.Exit(1)
	}

	var topo Topology
	if err := yaml.Unmarshal(topoData, &topo); err != nil {
		fmt.Fprintf(os.Stderr, "error parsing topology.yaml: %v\n", err)
		os.Exit(1)
	}

	var workers []WorkerInstance

	stages := make([]string, 0, len(topo.Pipelines))
	for stage := range topo.Pipelines {
		stages = append(stages, stage)
	}
	sort.Strings(stages)

	for _, stage := range stages {
		defs := topo.Pipelines[stage]
		for i, wd := range defs {
			for id := range wd.Amount {
				wi := WorkerInstance{
					ContainerName: fmt.Sprintf("%s_%s_%d", stage, wd.Name, id),
					WorkerPrefix:  fmt.Sprintf("%s_%s", stage, wd.Name),
					ID:            id,
					WorkerAmount:  wd.Amount,
					ConfigPath:    "/app/config.yaml",
					VolumeMapping: fmt.Sprintf("./configs/%s/%s.yaml:/app/config.yaml", stage, wd.Name),
				}

				if i+1 < len(defs) {
					next := defs[i+1]
					wi.HasNext = true
					wi.NextWorkerPrefix = fmt.Sprintf("%s_%s", stage, next.Name)
					wi.NextWorkerAmount = next.Amount
				}

				workers = append(workers, wi)
			}
		}
	}

	var clients []ClientInstance
	for id := range topo.Clients {
		clients = append(clients, ClientInstance{ID: id})
	}

	tmplData, err := os.ReadFile("configs/base-compose.yaml.tmpl")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading template file: %v\n", err)
		os.Exit(1)
	}

	tmpl, err := template.New("compose").Parse(string(tmplData))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing template: %v\n", err)
		os.Exit(1)
	}

	out, err := os.Create("docker-compose-dev.yaml")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer out.Close()

	if err := tmpl.Execute(out, TemplateData{Clients: clients, Workers: workers}); err != nil {
		fmt.Fprintf(os.Stderr, "error executing template: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("generated docker-compose-dev.yaml")
}
