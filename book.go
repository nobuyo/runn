package runn

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cli/safeexec"
	"github.com/goccy/go-json"
	"github.com/goccy/go-yaml"
	"github.com/k1LoW/sshc/v3"
)

const noDesc = "[No Description]"

type book struct {
	desc           string
	runners        map[string]interface{}
	vars           map[string]interface{}
	rawSteps       []map[string]interface{}
	debug          bool
	ifCond         string
	skipTest       bool
	funcs          map[string]interface{}
	stepKeys       []string
	path           string // runbook file path
	httpRunners    map[string]*httpRunner
	dbRunners      map[string]*dbRunner
	grpcRunners    map[string]*grpcRunner
	cdpRunners     map[string]*cdpRunner
	sshRunners     map[string]*sshRunner
	shellRunners   map[string]*shellRunner
	profile        bool
	intervalStr    string
	interval       time.Duration
	loop           *Loop
	useMap         bool
	t              *testing.T
	included       bool
	failFast       bool
	skipIncluded   bool
	grpcNoTLS      bool
	runMatch       *regexp.Regexp
	runSample      int
	runShardIndex  int
	runShardN      int
	runShuffle     bool
	runShuffleSeed int64
	runParallel    bool
	runParallelMax int64
	runRandom      int
	runnerErrs     map[string]error
	beforeFuncs    []func(*RunResult) error
	afterFuncs     []func(*RunResult) error
	capturers      capturers
	stdout         io.Writer
	stderr         io.Writer
}

func LoadBook(path string) (*book, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load runbook %s: %w", path, err)
	}
	bk, err := parseBook(f)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("failed to load runbook %s: %w", path, err)
	}
	bk.path = path
	if err := bk.parseRunners(); err != nil {
		return nil, err
	}
	if err := bk.parseVars(); err != nil {
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("failed to load runbook %s: %w", path, err)
	}

	return bk, nil
}

func (bk *book) Desc() string {
	return bk.desc
}

func (bk *book) If() string {
	return bk.ifCond
}

func (bk *book) parseRunners() error {
	for k, v := range bk.runners {
		if err := bk.parseRunner(k, v); err != nil {
			bk.runnerErrs[k] = err
		}
	}
	return nil
}

func (bk *book) parseVars() error {
	for k, v := range bk.vars {
		root, err := bk.generateOperatorRoot()
		if err != nil {
			return err
		}
		ev, err := evaluateSchema(v, root, nil)
		if err != nil {
			return err
		}
		bk.vars[k] = ev
	}
	return nil
}

func (bk *book) parseRunner(k string, v interface{}) error {
	delete(bk.runnerErrs, k)

	switch vv := v.(type) {
	case string:
		switch {
		case strings.HasPrefix(vv, "https://") || strings.HasPrefix(vv, "http://"):
			hc, err := newHTTPRunner(k, vv)
			if err != nil {
				return err
			}
			bk.httpRunners[k] = hc
		case strings.HasPrefix(vv, "grpc://"):
			addr := strings.TrimPrefix(vv, "grpc://")
			gc, err := newGrpcRunner(k, addr)
			if err != nil {
				return err
			}
			bk.grpcRunners[k] = gc
		case strings.HasPrefix(vv, "cdp://") || strings.HasPrefix(vv, "chrome://"):
			remote := strings.TrimPrefix(strings.TrimPrefix(vv, "cdp://"), "chrome://")
			cc, err := newCDPRunner(k, remote)
			if err != nil {
				return err
			}
			bk.cdpRunners[k] = cc
		case strings.HasPrefix(vv, "ssh://"):
			addr := strings.TrimPrefix(vv, "ssh://")
			sc, err := newSSHRunner(k, addr)
			if err != nil {
				return err
			}
			bk.sshRunners[k] = sc
		default:
			dc, err := newDBRunner(k, vv)
			if err != nil {
				return err
			}
			bk.dbRunners[k] = dc
		}
	case map[string]interface{}:
		tmp, err := yaml.Marshal(vv)
		if err != nil {
			return err
		}
		detect := false

		// HTTP Runner
		detect, err = bk.parseHTTPRunnerWithDetailed(k, tmp)
		if err != nil {
			return err
		}

		// gRPC Runner
		if !detect {
			detect, err = bk.parseGRPCRunnerWithDetailed(k, tmp)
			if err != nil {
				return err
			}
		}

		// SSH Runner
		if !detect {
			detect, err = bk.parseSSHRunnerWithDetailed(k, tmp)
			if err != nil {
				return err
			}
		}

		// Shell Runner
		if !detect {
			detect, err = bk.parseShellRunnerWithDetailed(k, tmp)
			if err != nil {
				return err
			}
		}

		if !detect {
			return fmt.Errorf("cannot detect runner: %s", string(tmp))
		}
	}

	return nil
}

func (bk *book) parseHTTPRunnerWithDetailed(name string, b []byte) (bool, error) {
	c := &httpRunnerConfig{}
	if err := yaml.Unmarshal(b, c); err != nil {
		return false, nil
	}
	if c.Endpoint == "" {
		return false, nil
	}
	root, err := bk.generateOperatorRoot()
	if err != nil {
		return false, err
	}
	r, err := newHTTPRunner(name, c.Endpoint)
	if err != nil {
		return false, err
	}
	if c.NotFollowRedirect {
		r.client.CheckRedirect = notFollowRedirectFn
	}
	if c.OpenApi3DocLocation != "" && !strings.HasPrefix(c.OpenApi3DocLocation, "https://") && !strings.HasPrefix(c.OpenApi3DocLocation, "http://") && !strings.HasPrefix(c.OpenApi3DocLocation, "/") {
		c.OpenApi3DocLocation = filepath.Join(root, c.OpenApi3DocLocation)
	}
	hv, err := newHttpValidator(c)
	if err != nil {
		return false, err
	}
	r.validator = hv
	bk.httpRunners[name] = r
	return true, nil
}

func (bk *book) parseGRPCRunnerWithDetailed(name string, b []byte) (bool, error) {
	c := &grpcRunnerConfig{}
	if err := yaml.Unmarshal(b, c); err != nil {
		return false, nil
	}
	if c.Addr == "" {
		return false, nil
	}
	root, err := bk.generateOperatorRoot()
	if err != nil {
		return false, err
	}
	r, err := newGrpcRunner(name, c.Addr)
	if err != nil {
		return false, err
	}
	r.tls = c.TLS
	if c.cacert != nil {
		r.cacert = c.cacert
	} else if strings.HasPrefix(c.CACert, "/") {
		b, err := os.ReadFile(c.CACert)
		if err != nil {
			return false, err
		}
		r.cacert = b
	} else {
		b, err := os.ReadFile(filepath.Join(root, c.CACert))
		if err != nil {
			return false, err
		}
		r.cacert = b
	}
	if c.cert != nil {
		r.cert = c.cert
	} else if strings.HasPrefix(c.Cert, "/") {
		b, err := os.ReadFile(c.Cert)
		if err != nil {
			return false, err
		}
		r.cert = b
	} else {
		b, err := os.ReadFile(filepath.Join(root, c.Cert))
		if err != nil {
			return false, err
		}
		r.cert = b
	}
	if c.key != nil {
		r.key = c.key
	} else if strings.HasPrefix(c.Key, "/") {
		b, err := os.ReadFile(c.Key)
		if err != nil {
			return false, err
		}
		r.key = b
	} else {
		b, err := os.ReadFile(filepath.Join(root, c.Key))
		if err != nil {
			return false, err
		}
		r.key = b
	}
	r.skipVerify = c.SkipVerify
	bk.grpcRunners[name] = r
	return true, nil
}

func (bk *book) parseSSHRunnerWithDetailed(name string, b []byte) (bool, error) {
	c := &sshRunnerConfig{}
	if err := yaml.Unmarshal(b, c); err != nil {
		return false, nil
	}
	if c.Host == "" && c.Hostname == "" {
		return false, nil
	}
	host := c.Host
	if host == "" {
		host = c.Hostname
	}
	root, err := bk.generateOperatorRoot()
	if err != nil {
		return false, err
	}
	opts := []sshc.Option{}
	if c.SSHConfig != "" {
		p := c.SSHConfig
		if !strings.HasPrefix(c.SSHConfig, "/") {
			p = filepath.Join(root, c.SSHConfig)
		}
		opts = append(opts, sshc.ClearConfigPath(), sshc.ConfigPath(p))
	}
	if c.Hostname != "" {
		opts = append(opts, sshc.Hostname(c.Hostname))
	}
	if c.User != "" {
		opts = append(opts, sshc.User(c.User))
	}
	if c.Port != 0 {
		opts = append(opts, sshc.Port(c.Port))
	}
	if c.IdentityFile != "" {
		p := c.IdentityFile
		if !strings.HasPrefix(c.IdentityFile, "/") {
			p = filepath.Join(root, c.IdentityFile)
		}
		opts = append(opts, sshc.IdentityFile(p))
	}

	client, err := sshc.NewClient(host, opts...)
	if err != nil {
		return false, err
	}
	r := &sshRunner{
		name:        name,
		client:      client,
		keepSession: c.KeepSession,
	}

	if r.keepSession {
		if err := r.startSession(); err != nil {
			return false, err
		}
	}

	bk.sshRunners[name] = r
	return true, nil
}

func (bk *book) parseShellRunnerWithDetailed(name string, b []byte) (bool, error) {
	c := &shellRunnerConfig{}
	if err := yaml.Unmarshal(b, c); err != nil {
		return false, nil
	}

	shell, err := safeexec.LookPath(c.Shell)
	if err != nil {
		return false, err
	}
	r := &shellRunner{
		name:        name,
		shell:       shell,
		keepProcess: c.KeepProcess,
	}

	bk.shellRunners[name] = r
	return true, nil
}

func (bk *book) applyOptions(opts ...Option) error {
	opts = setupBuiltinFunctions(opts...)
	for _, opt := range opts {
		if err := opt(bk); err != nil {
			return err
		}
	}
	return nil
}

func (bk *book) generateOperatorRoot() (string, error) {
	if bk.path != "" {
		return filepath.Dir(bk.path), nil
	} else {
		wd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		return wd, nil
	}
}

func newBook() *book {
	return &book{
		runners:      map[string]interface{}{},
		vars:         map[string]interface{}{},
		rawSteps:     []map[string]interface{}{},
		funcs:        map[string]interface{}{},
		httpRunners:  map[string]*httpRunner{},
		dbRunners:    map[string]*dbRunner{},
		grpcRunners:  map[string]*grpcRunner{},
		cdpRunners:   map[string]*cdpRunner{},
		sshRunners:   map[string]*sshRunner{},
		shellRunners: map[string]*shellRunner{},
		interval:     0 * time.Second,
		runnerErrs:   map[string]error{},
		stdout:       os.Stdout,
		stderr:       os.Stderr,
	}
}

func parseBook(in io.Reader) (*book, error) {
	rb, err := ParseRunbook(in)
	if err != nil {
		return nil, err
	}
	bk, err := rb.toBook()
	if err != nil {
		return nil, err
	}

	// To match behavior with json.Marshal
	{
		b, err := json.Marshal(bk.vars)
		if err != nil {
			return nil, fmt.Errorf("invalid vars: %w", err)
		}
		if err := json.Unmarshal(b, &bk.vars); err != nil {
			return nil, fmt.Errorf("invalid vars: %w", err)
		}
	}

	if bk.desc == "" {
		bk.desc = noDesc
	}

	if bk.intervalStr != "" {
		d, err := parseDuration(bk.intervalStr)
		if err != nil {
			return nil, fmt.Errorf("invalid interval: %w", err)
		}
		bk.interval = d
	}

	for k := range bk.runners {
		if err := validateRunnerKey(k); err != nil {
			return nil, err
		}
	}

	for i, s := range bk.rawSteps {
		if err := validateStepKeys(s); err != nil {
			return nil, fmt.Errorf("invalid steps[%d]. %w: %s", i, err, s)
		}
	}

	return bk, nil
}

func validateRunnerKey(k string) error {
	if k == includeRunnerKey || k == testRunnerKey || k == dumpRunnerKey || k == execRunnerKey || k == bindRunnerKey {
		return fmt.Errorf("runner name '%s' is reserved for built-in runner", k)
	}
	if k == ifSectionKey || k == descSectionKey || k == loopSectionKey {
		return fmt.Errorf("runner name '%s' is reserved for built-in section", k)
	}
	return nil
}

func validateStepKeys(s map[string]interface{}) error {
	if len(s) == 0 {
		return errors.New("step must specify at least one runner")
	}
	custom := 0
	for k := range s {
		if k == testRunnerKey || k == dumpRunnerKey || k == bindRunnerKey || k == ifSectionKey || k == descSectionKey || k == loopSectionKey {
			continue
		}
		custom += 1
	}
	if custom > 1 {
		return errors.New("runners that cannot be running at the same time are specified")
	}
	return nil
}
