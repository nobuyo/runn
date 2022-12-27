package runn

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Songmu/prompter"
	"github.com/cli/safeexec"
	"github.com/jhump/protoreflect/desc"
	"github.com/k1LoW/runn/builtin"
	"github.com/k1LoW/sshc/v3"
	"github.com/spf13/cast"
	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc"
)

type Option func(*book) error

// Book - Load runbook
func Book(path string) Option {
	return func(bk *book) error {
		loaded, err := LoadBook(path)
		if err != nil {
			return err
		}
		bk.path = loaded.path
		bk.desc = loaded.desc
		bk.ifCond = loaded.ifCond
		bk.useMap = loaded.useMap
		for k, r := range loaded.runners {
			bk.runners[k] = r
		}
		for k, r := range loaded.httpRunners {
			bk.httpRunners[k] = r
		}
		for k, r := range loaded.dbRunners {
			bk.dbRunners[k] = r
		}
		for k, r := range loaded.grpcRunners {
			bk.grpcRunners[k] = r
		}
		for k, r := range loaded.cdpRunners {
			bk.cdpRunners[k] = r
		}
		for k, r := range loaded.sshRunners {
			bk.sshRunners[k] = r
		}
		for k, r := range loaded.shellRunners {
			bk.shellRunners[k] = r
		}
		for k, v := range loaded.vars {
			bk.vars[k] = v
		}
		bk.runnerErrs = loaded.runnerErrs
		bk.rawSteps = loaded.rawSteps
		bk.stepKeys = loaded.stepKeys
		if !bk.debug {
			bk.debug = loaded.debug
		}
		if !bk.skipTest {
			bk.skipTest = loaded.skipTest
		}
		bk.loop = loaded.loop
		bk.grpcNoTLS = loaded.grpcNoTLS
		if loaded.intervalStr != "" {
			bk.interval = loaded.interval
		}
		return nil
	}
}

// Overlay - Overlay values on a runbook
func Overlay(path string) Option {
	return func(bk *book) error {
		if len(bk.rawSteps) == 0 {
			return errors.New("overlays are unusable without its base runbook")
		}
		loaded, err := LoadBook(path)
		if err != nil {
			return err
		}
		bk.desc = loaded.desc
		bk.ifCond = loaded.ifCond
		if len(loaded.rawSteps) > 0 {
			if bk.useMap != loaded.useMap {
				return errors.New("only runbooks of the same type can be layered")
			}
		}
		for k, r := range loaded.runners {
			bk.runners[k] = r
		}
		for k, r := range loaded.httpRunners {
			bk.httpRunners[k] = r
		}
		for k, r := range loaded.dbRunners {
			bk.dbRunners[k] = r
		}
		for k, r := range loaded.grpcRunners {
			bk.grpcRunners[k] = r
		}
		for k, r := range loaded.cdpRunners {
			bk.cdpRunners[k] = r
		}
		for k, r := range loaded.sshRunners {
			bk.sshRunners[k] = r
		}
		for k, r := range loaded.shellRunners {
			bk.shellRunners[k] = r
		}
		for k, v := range loaded.vars {
			bk.vars[k] = v
		}
		for k, e := range loaded.runnerErrs {
			bk.runnerErrs[k] = e
		}
		bk.rawSteps = append(bk.rawSteps, loaded.rawSteps...)
		bk.stepKeys = append(bk.stepKeys, loaded.stepKeys...)
		bk.debug = loaded.debug
		bk.skipTest = loaded.skipTest
		bk.loop = loaded.loop
		bk.grpcNoTLS = loaded.grpcNoTLS
		bk.interval = loaded.interval
		return nil
	}
}

// Underlay - Lay values under the runbook.
func Underlay(path string) Option {
	return func(bk *book) error {
		if len(bk.rawSteps) == 0 {
			return errors.New("underlays are unusable without its base runbook")
		}
		loaded, err := LoadBook(path)
		if err != nil {
			return err
		}
		if bk.desc == "" {
			bk.desc = loaded.desc
		}
		if bk.ifCond == "" {
			bk.ifCond = loaded.ifCond
		}
		if len(loaded.rawSteps) > 0 {
			if bk.useMap != loaded.useMap {
				return errors.New("only runbooks of the same type can be layered")
			}
		}
		for k, r := range loaded.runners {
			if _, ok := bk.runners[k]; !ok {
				bk.runners[k] = r
			}
		}
		for k, r := range loaded.httpRunners {
			if _, ok := bk.httpRunners[k]; !ok {
				bk.httpRunners[k] = r
			}
		}
		for k, r := range loaded.dbRunners {
			if _, ok := bk.dbRunners[k]; !ok {
				bk.dbRunners[k] = r
			}
		}
		for k, r := range loaded.grpcRunners {
			if _, ok := bk.grpcRunners[k]; !ok {
				bk.grpcRunners[k] = r
			}
		}
		for k, r := range loaded.cdpRunners {
			if _, ok := bk.cdpRunners[k]; !ok {
				bk.cdpRunners[k] = r
			}
		}
		for k, r := range loaded.sshRunners {
			if _, ok := bk.sshRunners[k]; !ok {
				bk.sshRunners[k] = r
			}
		}
		for k, r := range loaded.shellRunners {
			if _, ok := bk.shellRunners[k]; !ok {
				bk.shellRunners[k] = r
			}
		}
		for k, v := range loaded.vars {
			if _, ok := bk.vars[k]; !ok {
				bk.vars[k] = v
			}
		}
		for k, e := range loaded.runnerErrs {
			bk.runnerErrs[k] = e
		}
		bk.rawSteps = append(loaded.rawSteps, bk.rawSteps...)
		bk.stepKeys = append(loaded.stepKeys, bk.stepKeys...)
		if bk.intervalStr == "" {
			bk.interval = loaded.interval
		}
		bk.stdout = loaded.stdout
		bk.stderr = loaded.stderr
		return nil
	}
}

// Desc - Set description to runbook
func Desc(desc string) Option {
	return func(bk *book) error {
		bk.desc = desc
		return nil
	}
}

// Runner - Set runner to runbook
func Runner(name, dsn string, opts ...httpRunnerOption) Option {
	return func(bk *book) error {
		delete(bk.runnerErrs, name)
		if len(opts) == 0 {
			if err := validateRunnerKey(name); err != nil {
				return err
			}
			if err := bk.parseRunner(name, dsn); err != nil {
				bk.runnerErrs[name] = err
			}
			return nil
		}
		// HTTP Runner
		c := &httpRunnerConfig{}
		for _, opt := range opts {
			if err := opt(c); err != nil {
				bk.runnerErrs[name] = err
				return nil
			}
		}
		r, err := newHTTPRunner(name, dsn)
		if err != nil {
			bk.runnerErrs[name] = err
			return nil
		}
		if c.NotFollowRedirect {
			r.client.CheckRedirect = notFollowRedirectFn
		}
		r.multipartBoundary = c.MultipartBoundary
		if c.OpenApi3DocLocation != "" {
			v, err := newHttpValidator(c)
			if err != nil {
				bk.runnerErrs[name] = err
				return nil
			}
			r.validator = v
		}
		bk.httpRunners[name] = r
		return nil
	}
}

// HTTPRunner - Set HTTP runner to runbook
func HTTPRunner(name, endpoint string, client *http.Client, opts ...httpRunnerOption) Option {
	return func(bk *book) error {
		delete(bk.runnerErrs, name)
		r, err := newHTTPRunner(name, endpoint)
		if err != nil {
			return err
		}
		r.client = client
		bk.httpRunners[name] = r
		if len(opts) == 0 {
			return nil
		}
		c := &httpRunnerConfig{}
		for _, opt := range opts {
			if err := opt(c); err != nil {
				bk.runnerErrs[name] = err
				return nil
			}
		}
		if c.NotFollowRedirect {
			r.client.CheckRedirect = notFollowRedirectFn
		}
		r.multipartBoundary = c.MultipartBoundary
		v, err := newHttpValidator(c)
		if err != nil {
			bk.runnerErrs[name] = err
			return nil
		}
		r.validator = v
		return nil
	}
}

// HTTPRunnerWithHandler - Set HTTP runner to runbook with http.Handler
func HTTPRunnerWithHandler(name string, h http.Handler, opts ...httpRunnerOption) Option {
	return func(bk *book) error {
		delete(bk.runnerErrs, name)
		r, err := newHTTPRunnerWithHandler(name, h)
		if err != nil {
			bk.runnerErrs[name] = err
			return nil
		}
		if len(opts) > 0 {
			c := &httpRunnerConfig{}
			for _, opt := range opts {
				if err := opt(c); err != nil {
					bk.runnerErrs[name] = err
					return nil
				}
			}
			if c.NotFollowRedirect {
				bk.runnerErrs[name] = errors.New("HTTPRunnerWithHandler does not support option NotFollowRedirect")
				return nil
			}
			r.multipartBoundary = c.MultipartBoundary
			v, err := newHttpValidator(c)
			if err != nil {
				bk.runnerErrs[name] = err
				return nil
			}
			r.validator = v
		}
		bk.httpRunners[name] = r
		return nil
	}
}

// DBRunner - Set DB runner to runbook
func DBRunner(name string, client Querier) Option {
	return func(bk *book) error {
		delete(bk.runnerErrs, name)
		nt, err := nestTx(client)
		if err != nil {
			return err
		}
		bk.dbRunners[name] = &dbRunner{
			name:   name,
			client: nt,
		}
		return nil
	}
}

// GrpcRunner - Set gRPC runner to runbook
func GrpcRunner(name string, cc *grpc.ClientConn, opts ...grpcRunnerOption) Option {
	return func(bk *book) error {
		delete(bk.runnerErrs, name)
		r := &grpcRunner{
			name: name,
			cc:   cc,
			mds:  map[string]*desc.MethodDescriptor{},
		}
		if len(opts) > 0 {
			c := &grpcRunnerConfig{}
			for _, opt := range opts {
				if err := opt(c); err != nil {
					bk.runnerErrs[name] = err
					return nil
				}
			}
			r.tls = c.TLS
			if c.cacert != nil {
				r.cacert = c.cacert
			} else if c.CACert != "" {
				b, err := os.ReadFile(c.CACert)
				if err != nil {
					bk.runnerErrs[name] = err
					return nil
				}
				r.cacert = b
			}
			if c.cert != nil {
				r.cert = c.cert
			} else if c.Cert != "" {
				b, err := os.ReadFile(c.Cert)
				if err != nil {
					bk.runnerErrs[name] = err
					return nil
				}
				r.cert = b
			}
			if c.key != nil {
				r.key = c.key
			} else if c.Key != "" {
				b, err := os.ReadFile(c.Key)
				if err != nil {
					bk.runnerErrs[name] = err
					return nil
				}
				r.key = b
			}
			r.skipVerify = c.SkipVerify
		}
		bk.grpcRunners[name] = r
		return nil
	}
}

// SSHRunner - Set SSH runner to runbook
func SSHRunner(name string, client *ssh.Client) Option {
	return func(bk *book) error {
		delete(bk.runnerErrs, name)
		r := &sshRunner{
			name:   name,
			client: client,
		}
		if r.keepSession {
			if err := r.startSession(); err != nil {
				return err
			}
		}
		bk.sshRunners[name] = r
		return nil
	}
}

// SSHRunnerWithOptions - Set SSH runner to runbook using options
func SSHRunnerWithOptions(name string, opts ...sshRunnerOption) Option {
	return func(bk *book) error {
		delete(bk.runnerErrs, name)
		c := &sshRunnerConfig{}
		for _, opt := range opts {
			if err := opt(c); err != nil {
				return err
			}
		}
		if c.Host == "" && c.Hostname == "" {
			return fmt.Errorf("invalid SSH runner '%s': host or hostname is required", name)
		}
		host := c.Host
		if host == "" {
			host = c.Hostname
		}
		opts := []sshc.Option{}
		if c.SSHConfig != "" {
			p := c.SSHConfig
			if !strings.HasPrefix(c.SSHConfig, "/") {
				p = filepath.Join(filepath.Dir(bk.path), c.SSHConfig)
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
				p = filepath.Join(filepath.Dir(bk.path), c.IdentityFile)
			}
			opts = append(opts, sshc.IdentityFile(p))
		}

		client, err := sshc.NewClient(host, opts...)
		if err != nil {
			return err
		}

		r := &sshRunner{
			name:        name,
			client:      client,
			keepSession: c.KeepSession,
		}

		if r.keepSession {
			if err := r.startSession(); err != nil {
				return err
			}
		}

		bk.sshRunners[name] = r
		return nil
	}
}

// ShellRunnerWithOptions - Set Shell runner to runbook using options
func ShellRunnerWithOptions(name string, opts ...shellRunnerOption) Option {
	return func(bk *book) error {
		delete(bk.runnerErrs, name)
		c := &shellRunnerConfig{}
		for _, opt := range opts {
			if err := opt(c); err != nil {
				return err
			}
		}
		if c.Shell == "" {
			return fmt.Errorf("invalid Shell runner '%s': shell is required", name)
		}

		shell, err := safeexec.LookPath(c.Shell)
		if err != nil {
			return err
		}

		r := &shellRunner{
			name:        name,
			shell:       shell,
			keepProcess: c.KeepProcess,
		}

		bk.shellRunners[name] = r
		return nil
	}
}

// T - Acts as test helper
func T(t *testing.T) Option {
	return func(bk *book) error {
		bk.t = t
		return nil
	}
}

// Var - Set variable to runner
func Var(k interface{}, v interface{}) Option {
	return func(bk *book) error {
		root, err := bk.generateOperatorRoot()
		if err != nil {
			return err
		}
		ev, err := evaluateSchema(v, root, nil)
		if err != nil {
			return err
		}
		switch kk := k.(type) {
		case string:
			bk.vars[kk] = ev
		case []string:
			vars := bk.vars
			for _, kkk := range kk[:len(kk)-1] {
				_, ok := vars[kkk]
				if !ok {
					vars[kkk] = map[string]interface{}{}
				}
				m, ok := vars[kkk].(map[string]interface{})
				if !ok {
					// clear current vars to override
					vars[kkk] = map[string]interface{}{}
					m, _ = vars[kkk].(map[string]interface{})
				}
				vars = m
			}
			vars[kk[len(kk)-1]] = ev
		default:
			return fmt.Errorf("invalid key of var: %v", k)
		}
		return nil
	}
}

// Func - Set function to runner
func Func(k string, v interface{}) Option {
	return func(bk *book) error {
		bk.funcs[k] = v
		return nil
	}
}

// Debug - Enable debug output
func Debug(debug bool) Option {
	return func(bk *book) error {
		if !bk.debug {
			bk.debug = debug
		}
		return nil
	}
}

// Profile - Enable profile output
func Profile(profile bool) Option {
	return func(bk *book) error {
		if !bk.profile {
			bk.profile = profile
		}
		return nil
	}
}

// Interval - Set interval between steps
func Interval(d time.Duration) Option {
	return func(bk *book) error {
		if d < 0 {
			return fmt.Errorf("invalid interval: %s", d)
		}
		bk.interval = d
		return nil
	}
}

// FailFast - Enable fail-fast
func FailFast(enable bool) Option {
	return func(bk *book) error {
		bk.failFast = enable
		return nil
	}
}

// SkipIncluded - Skip running the included step by itself.
func SkipIncluded(enable bool) Option {
	return func(bk *book) error {
		bk.skipIncluded = enable
		return nil
	}
}

// SkipTest - Skip test section
func SkipTest(enable bool) Option {
	return func(bk *book) error {
		if !bk.skipTest {
			bk.skipTest = enable
		}
		return nil
	}
}

// GRPCNoTLS - Disable TLS use in all gRPC runners
func GRPCNoTLS(noTLS bool) Option {
	return func(bk *book) error {
		bk.grpcNoTLS = noTLS
		return nil
	}
}

// BeforeFunc - Register the function to be run before the runbook is run.
func BeforeFunc(fn func(*RunResult) error) Option {
	return func(bk *book) error {
		bk.beforeFuncs = append(bk.beforeFuncs, fn)
		return nil
	}
}

// AfterFunc - Register the function to be run after the runbook is run.
func AfterFunc(fn func(*RunResult) error) Option {
	return func(bk *book) error {
		bk.afterFuncs = append(bk.afterFuncs, fn)
		return nil
	}
}

// AfterFuncIf - Register the function to be run after the runbook is run if condition is true.
func AfterFuncIf(fn func(*RunResult) error, ifCond string) Option {
	return func(bk *book) error {
		bk.afterFuncs = append(bk.afterFuncs, func(r *RunResult) error {
			tf, err := EvalCond(ifCond, r.Store)
			if err != nil {
				return err
			}
			if !tf {
				return nil
			}
			return fn(r)
		})
		return nil
	}
}

// Capture - Register the capturer to capture steps.
func Capture(c Capturer) Option {
	return func(bk *book) error {
		bk.capturers = append(bk.capturers, c)
		return nil
	}
}

// RunMatch - Run only runbooks with matching paths.
func RunMatch(m string) Option {
	return func(bk *book) error {
		re, err := regexp.Compile(m)
		if err != nil {
			return err
		}
		bk.runMatch = re
		return nil
	}
}

// RunSample - Sample the specified number of runbooks.
func RunSample(n int) Option {
	return func(bk *book) error {
		if n <= 0 {
			return fmt.Errorf("sample must be greater than 0: %d", n)
		}
		bk.runSample = n
		return nil
	}
}

// RunShard - Distribute runbooks into a specified number of shards and run the specified shard of them.
func RunShard(n, i int) Option {
	return func(bk *book) error {
		if n <= 0 {
			return fmt.Errorf("the number of divisions is greater than 0: %d", n)
		}
		if i < 0 {
			return fmt.Errorf("the index of divisions is greater than or equal to 0: %d", i)
		}
		if i >= n {
			return fmt.Errorf("the index of divisions is less than the number of distributions (%d): %d", n, i)
		}
		bk.runShardIndex = i
		bk.runShardN = n
		return nil
	}
}

// RunShuffle - Randomize the order of running runbooks.
func RunShuffle(enable bool, seed int64) Option {
	return func(bk *book) error {
		bk.runShuffle = enable
		bk.runShuffleSeed = seed
		return nil
	}
}

// RunParallel - Parallelize runs of runbooks.
func RunParallel(enable bool, max int64) Option {
	return func(bk *book) error {
		bk.runParallel = enable
		bk.runParallelMax = max
		return nil
	}
}

// RunRandom - Run the specified number of runbooks at random. Sometimes the same runbook is run multiple times.
func RunRandom(n int) Option {
	return func(bk *book) error {
		if n <= 0 {
			return fmt.Errorf("ramdom must be greater than 0: %d", n)
		}
		bk.runRandom = n
		return nil
	}
}

// Stdout - Set STDOUT
func Stdout(w io.Writer) Option {
	return func(bk *book) error {
		bk.stdout = w
		return nil
	}
}

// Stderr - Set STDERR
func Stderr(w io.Writer) Option {
	return func(bk *book) error {
		bk.stderr = w
		return nil
	}
}

// setupBuiltinFunctions - Set up built-in functions to runner
func setupBuiltinFunctions(opts ...Option) []Option {
	// Built-in functions are added at the beginning of an option and are overridden by subsequent options
	return append([]Option{
		// NOTE: Please add here the built-in functions you want to enable.
		Func("urlencode", url.QueryEscape),
		Func("base64encode", func(v interface{}) string { return base64.StdEncoding.EncodeToString([]byte(cast.ToString(v))) }),
		Func("base64decode", func(v interface{}) string {
			decoded, _ := base64.StdEncoding.DecodeString(cast.ToString(v))
			return string(decoded)
		}),
		Func("string", func(v interface{}) string { return cast.ToString(v) }),
		Func("int", func(v interface{}) int { return cast.ToInt(v) }),
		Func("bool", func(v interface{}) bool { return cast.ToBool(v) }),
		Func("time", builtin.Time),
		Func("compare", builtin.Compare),
		Func("diff", builtin.Diff),
		Func("input", func(msg, defaultMsg interface{}) string {
			return prompter.Prompt(cast.ToString(msg), cast.ToString(defaultMsg))
		}),
		Func("secret", func(msg interface{}) string {
			return prompter.Password(cast.ToString(msg))
		}),
		Func("select", func(msg interface{}, list []interface{}, defaultSelect interface{}) string {
			choices := []string{}
			for _, v := range list {
				choices = append(choices, cast.ToString(v))
			}
			return prompter.Choose(cast.ToString(msg), choices, cast.ToString(defaultSelect))
		}),
	},
		opts...,
	)
}

func included(included bool) Option {
	return func(bk *book) error {
		bk.included = included
		return nil
	}
}

// Books - Load multiple runbooks.
func Books(pathp string) ([]Option, error) {
	paths, err := Paths(pathp)
	if err != nil {
		return nil, err
	}
	opts := []Option{}
	for _, p := range paths {
		opts = append(opts, Book(p))
	}
	return opts, nil
}

func GetDesc(opt Option) (string, error) {
	b := newBook()
	if err := opt(b); err != nil {
		return "", err
	}
	return b.desc, nil
}

func runnHTTPRunner(name string, r *httpRunner) Option {
	return func(bk *book) error {
		bk.httpRunners[name] = r
		return nil
	}
}

func runnDBRunner(name string, r *dbRunner) Option {
	return func(bk *book) error {
		bk.dbRunners[name] = r
		return nil
	}
}

func runnGrpcRunner(name string, r *grpcRunner) Option {
	return func(bk *book) error {
		bk.grpcRunners[name] = r
		return nil
	}
}

var (
	AsTestHelper = T
	Runbook      = Book
	RunPart      = RunShard
)
