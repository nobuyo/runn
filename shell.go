package runn

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

const shellOutTimeout = 1 * time.Second

type shellRunner struct {
	name        string
	shell       string
	cmd         *exec.Cmd
	stdin       io.WriteCloser
	stdout      chan string
	stderr      chan string
	keepProcess bool
	operator    *operator
}

type shellCommand struct {
	command string
	stdin   string
}

func (rnr *shellRunner) startProcess(ctx context.Context) error {
	if !rnr.keepProcess {
		return errors.New("could not use startProcess() when keepProcess = false")
	}

	cmd := exec.CommandContext(ctx, rnr.shell)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	ol := make(chan string)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			ol <- scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			panic(err)
		}
		close(ol)
	}()

	el := make(chan string)
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			el <- scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			panic(err)
		}
		close(el)
	}()

	if err := cmd.Start(); err != nil {
		return err
	}

	rnr.cmd = cmd
	rnr.stdin = stdin
	rnr.stdout = ol
	rnr.stderr = el
	return nil
}

func (rnr *shellRunner) terminateProcess() error {
	if rnr.cmd == nil {
		return nil
	}
	if rnr.cmd.Process == nil {
		return nil
	}
	if err := rnr.cmd.Process.Kill(); err != nil {
		return err
	}
	rnr.cmd = nil
	rnr.stdin = nil
	rnr.stdout = nil
	rnr.stderr = nil
	return nil
}

func (rnr *shellRunner) Close() error {
	return rnr.terminateProcess()
}

func (rnr *shellRunner) Run(ctx context.Context, c *shellCommand) error {
	if !rnr.keepProcess {
		return rnr.runOnce(ctx, c)
	}

	if rnr.cmd == nil {
		if err := rnr.startProcess(ctx); err != nil {
			return err
		}
	}

	rnr.operator.capturers.captureShellCommand(c.command)
	stdout := ""
	stderr := ""
	if _, err := fmt.Fprintf(rnr.stdin, "%s\n", strings.TrimRight(c.command, "\n")); err != nil {
		return err
	}
	if strings.Trim(c.stdin, " \n") != "" {
		if _, err := fmt.Fprintf(rnr.stdin, "%s\n", c.stdin); err != nil {
			return err
		}

		rnr.operator.capturers.captureShellStdin(c.stdin)
	}

	timer := time.NewTimer(0)

L:
	for {
		timer.Reset(shellOutTimeout)
		select {
		case line, ok := <-rnr.stdout:
			if !ok {
				break L
			}
			stdout += fmt.Sprintf("%s\n", line)
		case line, ok := <-rnr.stderr:
			if !ok {
				break L
			}
			stderr += fmt.Sprintf("%s\n", line)
		case <-timer.C:
			break L
		case <-ctx.Done():
			break L
		}
	}

	rnr.operator.capturers.captureShellStdout(stdout)
	rnr.operator.capturers.captureShellStderr(stderr)

	rnr.operator.record(map[string]interface{}{
		"stdout": stdout,
		"stderr": stderr,
	})
	return nil
}

func (rnr *shellRunner) runOnce(ctx context.Context, c *shellCommand) error {
	rnr.operator.capturers.captureShellCommand(c.command)

	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)

	cmd := exec.CommandContext(ctx, rnr.shell, "-c", c.command)
	if strings.Trim(c.stdin, " \n") != "" {
		cmd.Stdin = strings.NewReader(c.stdin)

		rnr.operator.capturers.captureShellStdin(c.stdin)
	}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	rnr.cmd = cmd
	defer func() {
		_ = rnr.terminateProcess()
	}()

	_ = rnr.cmd.Run()

	rnr.operator.capturers.captureShellStdout(stdout.String())
	rnr.operator.capturers.captureShellStderr(stderr.String())

	rnr.operator.record(map[string]interface{}{
		"stdout": stdout.String(),
		"stderr": stderr.String(),
	})

	return nil
}
