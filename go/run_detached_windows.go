//go:build windows

package main

import (
	"errors"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

func configureDetached(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: 0x00000008 | 0x00000200, // DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP
	}
}

func killProcessTree(pid int32) error {
	if pid <= 1 {
		return errors.New("拒绝结束无效或系统进程 PID")
	}
	cmd := exec.Command("taskkill", "/PID", strconv.Itoa(int(pid)), "/T", "/F")
	if output, err := cmd.CombinedOutput(); err != nil {
		return errors.New(strings.TrimSpace(string(output)) + ": " + err.Error())
	}
	return nil
}
