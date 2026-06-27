package main

import (
	"bufio"
	"context"
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
	interval     time.Duration
	nukeMode     bool
	queryNum     int
	sniperTarget string
)

func init() {
	flag.DurationVar(&interval, "i", 5*time.Second, "Intervalo entre kills")
	flag.BoolVar(&nukeMode, "n", false, "Nuke all killable workers")
	flag.BoolVar(&nukeMode, "nuke", false, "Nuke all killable workers")
	flag.IntVar(&queryNum, "q", 0, "Numero de query a matar (activa modo query)")
	flag.StringVar(&sniperTarget, "s", "", "Contenedor objetivo en modo sniper")
}

func main() {
	flag.Parse()

	mode := "random"
	if sniperTarget != "" {
		mode = "sniper"
	} else if nukeMode {
		mode = "nuke"
	} else if queryNum > 0 {
		mode = "query"
	}

	slog.SetDefault(slog.New(newDemoHandler(os.Stderr, slog.LevelInfo)))
	slog.Info("chaos-monkey starting", "mode", mode, "interval", interval, "query_number", queryNum, "sniper_target", sniperTarget)

	if sniperTarget != "" {
		runSniper(sniperTarget)
		return
	}

	workers, err := loadWorkers()
	if err != nil {
		slog.Error("failed to load workers list", "error", err)
		os.Exit(1)
	}
	slog.Info("workers loaded", "count", len(workers))

	monitors, regulars := splitMonitors(workers)
	slog.Info("workers categorized", "monitors", len(monitors), "regular", len(regulars))

	shieldedID, err := resolveShielded(monitors, "last")
	if err != nil {
		slog.Error("invalid shielded configuration", "error", err)
		os.Exit(1)
	}
	slog.Info("shielded monitor resolved", "id", shieldedID, "container", fmt.Sprintf("monitor_%d", shieldedID))

	killable := buildKillable(regulars, monitors, shieldedID)
	slog.Info("killable pool", "count", len(killable))

	if nukeMode {
		runNuke(killable)
		return
	}

	if queryNum > 0 {
		slog.Info("query mode activated", "query_number", queryNum)
		targets := filterQuery(killable, queryNum)
		if len(targets) == 0 {
			slog.Warn("no workers found for query", "query", queryNum)
			return
		}
		slog.Info("query mode: killing workers", "query", queryNum, "targets", targets)
		bulkKill(targets)
		return
	}

	runRandom(interval, killable)
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
			slog.Info("random kill: selected target", "TARGET", target)
			go func() {
				if err := dockerKill(target); err != nil {
					slog.Error("docker kill failed", "CONTAINER", target, "error", err)
				}
			}()
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

func runSniper(container string) {
	slog.Info("sniper mode: watching container", "target", container)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "logs", "-f", "--tail=0", container)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		slog.Error("sniper: failed to create stdout pipe", "error", err)
		return
	}

	if err := cmd.Start(); err != nil {
		slog.Error("sniper: failed to start docker logs", "error", err)
		return
	}

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "SNIPER") {
			slog.Warn("SNIPER TARGET ACQUIRED", "container", container, "log_line", line)
			if err := dockerKill(container); err != nil {
				slog.Error("sniper: docker kill failed", "container", container, "error", err)
			} else {
				slog.Warn("SNIPER SHOT TAKEN", "container", container)
			}
			break
		}
	}

	if err := scanner.Err(); err != nil {
		slog.Error("sniper: error reading docker logs", "error", err)
	}

	cmd.Wait()
}
/* ------------------ En topology.yaml: ----------------- */
/*
env:
  SNIPER: "true"
*/

/* ---------------- En cualquier worker: ---------------- */
/*
if os.Getenv("SNIPER") == "true" {
		slog.Warn("[SNIPER] Sleeping to allow sniper to acquire target...")
		time.Sleep(5 * time.Second)
		slog.Info("I survived the Sniper")
}
*/

func dockerKill(container string) error {
	cmd := exec.Command("docker", "kill", container)
	return cmd.Run()
}
