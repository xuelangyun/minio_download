//go:build !windows

package main

import (
	"errors"
	"os/exec"
	"syscall"
)

func configureDetached(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}

func killProcessTree(pid int32) error {
	if pid <= 1 {
		return errors.New("拒绝结束无效或系统进程 PID")
	}
	pgid, err := syscall.Getpgid(int(pid))
	if err == nil && pgid == int(pid) {
		return syscall.Kill(-int(pid), syscall.SIGKILL)
	}
	// 手动启动的 CLI 不一定拥有独立进程组，只结束已核验的下载器 PID。
	return syscall.Kill(int(pid), syscall.SIGKILL)
}
