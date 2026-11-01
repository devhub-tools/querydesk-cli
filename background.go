package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/devhub-tools/querydesk-cli/internal/config"
)

// The listener runs as its own process, so `querydesk start` gives the terminal back. It
// writes its pid once it's listening and removes it when it stops; its output goes to the log.

func pidPath() (string, error) {
	dir, err := config.Dir()
	return filepath.Join(dir, "querydesk.pid"), err
}

func logPath() (string, error) {
	dir, err := config.Dir()
	return filepath.Join(dir, "querydesk.log"), err
}

// running is the pid of the listener, or 0 when none is.
func running() int {
	path, err := pidPath()
	if err != nil {
		return 0
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || !alive(pid) {
		return 0
	}
	return pid
}

func writePid() (func(), error) {
	path, err := pidPath()
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		return nil, err
	}
	return func() {
		// only its own: a newer listener may have replaced it
		if raw, err := os.ReadFile(path); err == nil && strings.TrimSpace(string(raw)) == strconv.Itoa(os.Getpid()) {
			os.Remove(path)
		}
	}, nil
}

func startBackground(ctx context.Context, verbose bool) error {
	cfg, client, err := setup()
	if err != nil {
		return err
	}
	if pid := running(); pid != 0 {
		fmt.Printf("Already running (pid %d).\n\n", pid)
		return printDatabases(ctx, cfg, client, os.Stdout)
	}
	// signed in, so the listener doesn't start only to fail where nobody sees it
	if _, err := client.Tokens.Token(ctx); err != nil {
		return err
	}

	logFile, err := logPath()
	if err != nil {
		return err
	}
	out, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer out.Close()
	// what this start writes, to show if it fails
	since, _ := out.Seek(0, io.SeekEnd)

	self, err := os.Executable()
	if err != nil {
		return err
	}
	args := []string{"start", "--foreground"}
	if verbose {
		args = append(args, "--verbose")
	}
	cmd := exec.Command(self, args...)
	cmd.Stdout, cmd.Stderr = out, out
	detach(cmd)
	if err := cmd.Start(); err != nil {
		return err
	}
	exited := make(chan struct{})
	go func() {
		cmd.Wait()
		close(exited)
	}()

	for deadline := time.Now().Add(15 * time.Second); running() != cmd.Process.Pid; {
		select {
		case <-exited:
			return fmt.Errorf("it stopped as it started:\n%s", written(logFile, since))
		case <-time.After(100 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("it didn't start listening within 15 seconds; see %s", logFile)
		}
	}

	fmt.Printf("Listening in the background (pid %d): Postgres clients on 127.0.0.1:%d, MySQL clients on 127.0.0.1:%d.\n",
		cmd.Process.Pid, cfg.PostgresPort, cfg.MySQLPort)
	fmt.Printf("Log: %s. Stop it with `querydesk stop`.\n\n", logFile)
	return printDatabases(ctx, cfg, client, os.Stdout)
}

func stopBackground() error {
	pid := running()
	if pid == 0 {
		fmt.Println("Not running.")
		return nil
	}
	if err := terminate(pid); err != nil {
		return err
	}
	for deadline := time.Now().Add(10 * time.Second); alive(pid); time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			return errors.New("it didn't stop within 10 seconds")
		}
	}
	if path, err := pidPath(); err == nil {
		os.Remove(path)
	}
	fmt.Printf("Stopped (pid %d).\n", pid)
	return nil
}

func status() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if pid := running(); pid != 0 {
		logFile, _ := logPath()
		fmt.Printf("Running (pid %d): Postgres clients on 127.0.0.1:%d, MySQL clients on 127.0.0.1:%d. Log: %s\n",
			pid, cfg.PostgresPort, cfg.MySQLPort, logFile)
		return nil
	}
	fmt.Println("Not running. Start it with `querydesk start`.")
	return nil
}

func written(path string, since int64) string {
	raw, err := os.ReadFile(path)
	if err != nil || since > int64(len(raw)) {
		return ""
	}
	return string(bytes.TrimRight(raw[since:], "\n"))
}
