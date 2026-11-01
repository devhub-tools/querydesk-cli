//go:build !windows

package main

import (
	"os/exec"
	"syscall"
)

// detach starts the listener in a session of its own, so closing the terminal doesn't stop it.
func detach(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} }

func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

func terminate(pid int) error { return syscall.Kill(pid, syscall.SIGTERM) }
