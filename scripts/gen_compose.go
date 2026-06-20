package main

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"text/template"

	"gopkg.in/yaml.v3"
)

type WorkerDef struct {
	Name    string            `yaml:"name"`
	Amount  int               `yaml:"amount"`
	Volumes []string          `yaml:"volumes"`
	Env     map[string]string `yaml:"env"`
}

type ClientGroup struct {
	Count   int               `yaml:"count"`
	Volumes []string          `yaml:"volumes"`
	Env     map[string]string `yaml:"env"`
}

type MonitorGroup struct {
	Amount int `yaml:"amount"`
}

type MonitorInstance struct {
	ContainerName string
	WorkerPrefix  string
	ID            int
	WorkerAmount  int
	EnvSorted     []string
}

type Topology struct {
	Env       map[string]string      `yaml:"env"`
	Clients   ClientGroup            `yaml:"clients"`
	Monitors  MonitorGroup           `yaml:"monitors"`
	Pipelines map[string][]WorkerDef `yaml:"pipelines"`
}

type WorkerInstance struct {
	ContainerName    string
	WorkerPrefix     string
	ID               int
	WorkerAmount     int
	ConfigPath       string
	VolumeMapping    string
	HasPrev          bool
	PrevWorkerPrefix string
	PrevWorkerAmount int
	HasNext          bool
	NextWorkerPrefix string
	NextWorkerAmount int
	ExtraVolumes     []string
	EnvSorted        []string
}

type ClientInstance struct {
	ID           int
	EnvSorted    []string
	ExtraVolumes []string
}

type TemplateData struct {
	Clients  []ClientInstance
	Monitors []MonitorInstance
	Workers  []WorkerInstance
}

func main() {
	if _, err := os.Stat("topology.yaml"); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "error: topology.yaml does not exist\n")
		os.Exit(1)
	}
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
					ExtraVolumes:  wd.Volumes,
				}

				if i > 0 {
					prev := defs[i-1]
					wi.HasPrev = true
					wi.PrevWorkerPrefix = fmt.Sprintf("%s_%s", stage, prev.Name)
					wi.PrevWorkerAmount = prev.Amount
				}

				if i+1 < len(defs) {
					next := defs[i+1]
					wi.HasNext = true
					wi.NextWorkerPrefix = fmt.Sprintf("%s_%s", stage, next.Name)
					wi.NextWorkerAmount = next.Amount
				}

				env := map[string]string{
					"LOG_LEVEL":     "debug",
					"CONFIG_PATH":   "/app/config.yaml",
					"WORKER_PREFIX": wi.WorkerPrefix,
					"ID":            strconv.Itoa(id),
					"WORKER_AMOUNT": strconv.Itoa(wd.Amount),
				}
				if wi.HasPrev {
					env["PREV_WORKER_PREFIX"] = wi.PrevWorkerPrefix
					env["PREV_WORKER_AMOUNT"] = strconv.Itoa(wi.PrevWorkerAmount)
				}
				if wi.HasNext {
					env["NEXT_WORKER_PREFIX"] = wi.NextWorkerPrefix
					env["NEXT_WORKER_AMOUNT"] = strconv.Itoa(wi.NextWorkerAmount)
				}
				for k, v := range topo.Env { // global, before per-stage so stages can override
					env[k] = v
				}
				for k, v := range wd.Env {
					env[k] = v
				}

				keys := make([]string, 0, len(env))
				for k := range env {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				for _, k := range keys {
					wi.EnvSorted = append(wi.EnvSorted, fmt.Sprintf("%s=%s", k, env[k]))
				}

				workers = append(workers, wi)
			}
		}
	}

	var clients []ClientInstance
	for id := range topo.Clients.Count {
		env := map[string]string{
			"LOG_LEVEL": "debug",
		}
		for k, v := range topo.Clients.Env {
			env[k] = v
		}

		keys := make([]string, 0, len(env))
		for k := range env {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		var envSorted []string
		for _, k := range keys {
			envSorted = append(envSorted, fmt.Sprintf("%s=%s", k, env[k]))
		}

		clients = append(clients, ClientInstance{
			ID:           id,
			EnvSorted:    envSorted,
			ExtraVolumes: topo.Clients.Volumes,
		})
	}

	var monitors []MonitorInstance
	if topo.Monitors.Amount < 3 {
		topo.Monitors.Amount = 3
	}
	for id := range topo.Monitors.Amount {
		env := map[string]string{
			"LOG_LEVEL":          "debug",
			"CONFIG_PATH":        "/app/config.yaml",
			"SYSTEM_CONFIG_PATH": "/app/system.yaml",
			"WORKER_PREFIX":      "monitor",
			"ID":                 strconv.Itoa(id),
			"WORKER_AMOUNT":      strconv.Itoa(topo.Monitors.Amount),
		}
		keys := make([]string, 0, len(env))
		for k := range env {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var envSorted []string
		for _, k := range keys {
			envSorted = append(envSorted, fmt.Sprintf("%s=%s", k, env[k]))
		}
		monitors = append(monitors, MonitorInstance{
			ContainerName: fmt.Sprintf("monitor_%d", id),
			WorkerPrefix:  "monitor",
			ID:            id,
			WorkerAmount:  topo.Monitors.Amount,
			EnvSorted:    envSorted,
		})
	}

	tmplData, err := os.ReadFile("configs/base-compose.yaml.tmpl")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading template file: %v\n", err)
		os.Exit(1)
	}

	tmpl, err := template.New("compose").Funcs(template.FuncMap{
		"until": func(n int) []int {
			r := make([]int, n)
			for i := 0; i < n; i++ {
				r[i] = i
			}
			return r
		},
	}).Parse(string(tmplData))
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

	if err := tmpl.Execute(out, TemplateData{Clients: clients, Monitors: monitors, Workers: workers}); err != nil {
		fmt.Fprintf(os.Stderr, "error executing template: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("generated docker-compose-dev.yaml")

	if err := writeWorkersYAML(workers, monitors); err != nil {
		fmt.Fprintf(os.Stderr, "error writing workers.yaml: %v\n", err)
		os.Exit(1)
	}
}

func writeWorkersYAML(workers []WorkerInstance, monitors []MonitorInstance) error {
	names := make([]string, 0, len(workers)+len(monitors))
	for _, w := range workers {
		names = append(names, w.ContainerName)
	}
	for _, m := range monitors {
		names = append(names, m.ContainerName)
	}
	data := map[string]any{"workers": names}
	out, err := yaml.Marshal(data)
	if err != nil {
		return err
	}
	return os.WriteFile("configs/workers.yaml", out, 0644)
}
