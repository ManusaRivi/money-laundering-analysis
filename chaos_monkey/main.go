package main

import (
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
)

type workersFile struct {
	Workers []string `yaml:"workers"`
}

var (
	mode     string
	interval time.Duration
	shielded string
	queryNum int
)

func init() {
	flag.StringVar(&mode, "m", "random", "Modo: random, nuke")
	flag.DurationVar(&interval, "i", 5*time.Second, "Intervalo entre kills")
	flag.StringVar(&shielded, "s", "last", "Monitor blindado: 'last' o numero de ID")
	flag.IntVar(&queryNum, "q", 0, "Numero de query a matar (activa modo query)")
}

func main() {
	flag.Parse()

	if queryNum > 0 {
		mode = "query"
	}

	slog.SetDefault(slog.New(newDemoHandler(os.Stderr, slog.LevelInfo)))
	slog.Info("chaos-monkey starting", "mode", mode, "interval", interval, "shielded", shielded)

	workers, err := loadWorkers()
	if err != nil {
		slog.Error("failed to load workers list", "error", err)
		os.Exit(1)
	}
	slog.Info("workers loaded", "count", len(workers))

	monitors, regulars := splitMonitors(workers)
	slog.Info("workers categorized", "monitors", len(monitors), "regular", len(regulars))

	shieldedID, err := resolveShielded(monitors, shielded)
	if err != nil {
		slog.Error("invalid shielded configuration", "error", err)
		os.Exit(1)
	}
	slog.Info("shielded monitor resolved", "id", shieldedID, "container", fmt.Sprintf("monitor_%d", shieldedID))

	killable := buildKillable(regulars, monitors, shieldedID)
	slog.Info("killable pool", "count", len(killable))

	switch mode {
	case "random":
		runRandom(interval, killable)
	case "nuke":
		runNuke(killable)
	case "query":
		slog.Info("query mode activated", "query_number", queryNum)
		targets := filterQuery(killable, queryNum)
		if len(targets) == 0 {
			slog.Warn("no workers found for query", "query", queryNum)
			return
		}
		slog.Info("query mode: killing workers", "query", queryNum, "targets", targets)
		bulkKill(targets)
	default:
		slog.Error("unknown mode", "mode", mode)
		flag.Usage()
		os.Exit(1)
	}
}

func loadWorkers() ([]string, error) {
	const path = "configs/workers.yaml"
	slog.Debug("reading workers file", "path", path)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read workers config: %w", err)
	}
	var wf workersFile
	if err := yaml.Unmarshal(data, &wf); err != nil {
		return nil, fmt.Errorf("parse workers config: %w", err)
	}
	return wf.Workers, nil
}

func splitMonitors(workers []string) (monitors, regulars []string) {
	for _, w := range workers {
		if strings.HasPrefix(w, "monitor_") {
			monitors = append(monitors, w)
		} else {
			regulars = append(regulars, w)
		}
	}
	return
}

func extractMonitorID(name string) (int, error) {
	parts := strings.Split(name, "_")
	if len(parts) < 2 {
		return 0, fmt.Errorf("invalid monitor name: %s", name)
	}
	return strconv.Atoi(parts[len(parts)-1])
}

func resolveShielded(monitors []string, shielded string) (int, error) {
	if shielded == "last" {
		maxID := -1
		for _, m := range monitors {
			id, err := extractMonitorID(m)
			if err != nil {
				slog.Warn("skipping invalid monitor name", "name", m, "error", err)
				continue
			}
			if id > maxID {
				maxID = id
			}
		}
		if maxID < 0 {
			return 0, fmt.Errorf("no monitors found to determine 'last'")
		}
		slog.Debug("resolved shielded as last monitor", "id", maxID)
		return maxID, nil
	}

	id, err := strconv.Atoi(shielded)
	if err != nil {
		return 0, fmt.Errorf("shielded must be 'last' or a numeric ID, got %q", shielded)
	}

	target := fmt.Sprintf("monitor_%d", id)
	for _, m := range monitors {
		if m == target {
			slog.Debug("resolved shielded monitor", "id", id, "container", target)
			return id, nil
		}
	}
	return 0, fmt.Errorf("shielded monitor %s not found in workers list", target)
}

func buildKillable(regulars, monitors []string, shieldedID int) []string {
	var killable []string
	killable = append(killable, regulars...)
	for _, m := range monitors {
		id, err := extractMonitorID(m)
		if err != nil {
			slog.Warn("skipping invalid monitor name in killable pool", "name", m, "error", err)
			continue
		}
		if id == shieldedID {
			slog.Debug("excluding shielded monitor from killable pool", "container", m)
			continue
		}
		killable = append(killable, m)
	}
	return killable
}

func filterQuery(killable []string, queryNum int) []string {
	re := regexp.MustCompile(fmt.Sprintf(`^query%d(?:[a-z]?)_`, queryNum))
	var targets []string
	for _, w := range killable {
		if re.MatchString(w) {
			targets = append(targets, w)
		}
	}
	return targets
}

func runRandom(interval time.Duration, killable []string) {
	if len(killable) == 0 {
		slog.Warn("no killable workers available, exiting")
		return
	}
	slog.Info("random mode: will kill one worker each tick", "interval", interval, "killable_count", len(killable))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-sigCh:
			slog.Info("chaos-monkey stopping (signal received)")
			return
		case <-time.After(interval):
			idx := rand.Intn(len(killable))
			target := killable[idx]
			slog.Info("random kill: selected target", "target", target)
			if err := dockerKill(target); err != nil {
				slog.Error("docker kill failed", "target", target, "error", err)
			}
		}
	}
}

func runNuke(killable []string) {
	if len(killable) == 0 {
		slog.Warn("no killable workers to nuke")
		return
	}
	slog.Warn("NUKE: killing all killable workers", "count", len(killable))
	bulkKill(killable)
	slog.Info("nuke complete")
}

func bulkKill(targets []string) {
	var wg sync.WaitGroup
	for _, t := range targets {
		wg.Add(1)
		go func(container string) {
			defer wg.Done()
			slog.Info("killing container", "container", container)
			if err := dockerKill(container); err != nil {
				slog.Error("docker kill failed", "container", container, "error", err)
			}
		}(t)
	}
	wg.Wait()
}

func dockerKill(container string) error {
	cmd := exec.Command("docker", "kill", container)
	return cmd.Run()
}
