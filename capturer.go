package runn

import (
	"net/http"

	"google.golang.org/grpc/metadata"
)

type Capturer interface {
	CaptureStart(ids IDs, bookPath, desc string)
	CaptureFailure(ids IDs, bookPath, desc string, err error)
	CaptureSkipped(ids IDs, bookPath, desc string)
	CaptureSuccess(ids IDs, bookPath, desc string)
	CaptureEnd(ids IDs, bookPath, desc string)

	CaptureHTTPRequest(name string, req *http.Request)
	CaptureHTTPResponse(name string, res *http.Response)

	CaptureGRPCStart(name string, typ GRPCType, service, method string)
	CaptureGRPCRequestHeaders(h map[string][]string)
	CaptureGRPCRequestMessage(m map[string]interface{})
	CaptureGRPCResponseStatus(status int)
	CaptureGRPCResponseHeaders(h map[string][]string)
	CaptureGRPCResponseMessage(m map[string]interface{})
	CaptureGRPCResponseTrailers(t map[string][]string)
	CaptureGRPCClientClose()
	CaptureGRPCEnd(name string, typ GRPCType, service, method string)

	CaptureCDPStart(name string)
	CaptureCDPAction(a CDPAction)
	CaptureCDPResponse(a CDPAction, res map[string]interface{})
	CaptureCDPEnd(name string)

	CaptureSSHCommand(command string)
	CaptureSSHStdout(stdout string)
	CaptureSSHStderr(stderr string)

	CaptureDBStatement(name string, stmt string)
	CaptureDBResponse(name string, res *DBResponse)

	CaptureExecCommand(command string)
	CaptureExecStdin(stdin string)
	CaptureExecStdout(stdout string)
	CaptureExecStderr(stderr string)

	CaptureShellCommand(command string)
	CaptureShellStdin(stdin string)
	CaptureShellStdout(stdout string)
	CaptureShellStderr(stderr string)

	SetCurrentIDs(ids IDs)
	Errs() error
}

type capturers []Capturer

func (cs capturers) captureStart(ids IDs, bookPath, desc string) {
	for _, c := range cs {
		c.CaptureStart(ids, bookPath, desc)
	}
}

func (cs capturers) captureFailure(ids IDs, bookPath, desc string, err error) {
	for _, c := range cs {
		c.CaptureFailure(ids, bookPath, desc, err)
	}
}

func (cs capturers) captureSkipped(ids IDs, bookPath, desc string) {
	for _, c := range cs {
		c.CaptureSkipped(ids, bookPath, desc)
	}
}

func (cs capturers) captureSuccess(ids IDs, bookPath, desc string) {
	for _, c := range cs {
		c.CaptureSuccess(ids, bookPath, desc)
	}
}

func (cs capturers) captureEnd(ids IDs, bookPath, desc string) {
	for _, c := range cs {
		c.CaptureEnd(ids, bookPath, desc)
	}
}

func (cs capturers) captureHTTPRequest(name string, req *http.Request) {
	for _, c := range cs {
		c.CaptureHTTPRequest(name, req)
	}
}

func (cs capturers) captureHTTPResponse(name string, res *http.Response) {
	for _, c := range cs {
		c.CaptureHTTPResponse(name, res)
	}
}

func (cs capturers) captureGRPCStart(name string, typ GRPCType, service, method string) {
	for _, c := range cs {
		c.CaptureGRPCStart(name, typ, service, method)
	}
}
func (cs capturers) captureGRPCRequestHeaders(h metadata.MD) {
	for _, c := range cs {
		c.CaptureGRPCRequestHeaders(h)
	}
}

func (cs capturers) captureGRPCRequestMessage(m map[string]interface{}) {
	for _, c := range cs {
		c.CaptureGRPCRequestMessage(m)
	}
}

func (cs capturers) captureGRPCResponseStatus(status int) {
	for _, c := range cs {
		c.CaptureGRPCResponseStatus(status)
	}
}

func (cs capturers) captureGRPCResponseHeaders(h metadata.MD) {
	for _, c := range cs {
		c.CaptureGRPCResponseHeaders(h)
	}
}

func (cs capturers) captureGRPCResponseMessage(m map[string]interface{}) {
	for _, c := range cs {
		c.CaptureGRPCResponseMessage(m)
	}
}

func (cs capturers) captureGRPCResponseTrailers(t metadata.MD) {
	for _, c := range cs {
		c.CaptureGRPCResponseTrailers(t)
	}
}

func (cs capturers) captureGRPCClientClose() {
	for _, c := range cs {
		c.CaptureGRPCClientClose()
	}
}

func (cs capturers) captureGRPCEnd(name string, typ GRPCType, service, method string) {
	for _, c := range cs {
		c.CaptureGRPCEnd(name, typ, service, method)
	}
}

func (cs capturers) captureCDPStart(name string) {
	for _, c := range cs {
		c.CaptureCDPStart(name)
	}
}

func (cs capturers) captureCDPAction(a CDPAction) {
	for _, c := range cs {
		c.CaptureCDPAction(a)
	}
}

func (cs capturers) captureCDPResponse(a CDPAction, res map[string]interface{}) {
	for _, c := range cs {
		c.CaptureCDPResponse(a, res)
	}
}

func (cs capturers) captureCDPEnd(name string) {
	for _, c := range cs {
		c.CaptureCDPEnd(name)
	}
}

func (cs capturers) captureSSHCommand(command string) {
	for _, c := range cs {
		c.CaptureSSHCommand(command)
	}
}

func (cs capturers) captureSSHStdout(stdout string) {
	for _, c := range cs {
		c.CaptureSSHStdout(stdout)
	}
}

func (cs capturers) captureSSHStderr(stderr string) {
	for _, c := range cs {
		c.CaptureSSHStderr(stderr)
	}
}

func (cs capturers) captureDBStatement(name string, stmt string) {
	for _, c := range cs {
		c.CaptureDBStatement(name, stmt)
	}
}

func (cs capturers) captureDBResponse(name string, res *DBResponse) {
	for _, c := range cs {
		c.CaptureDBResponse(name, res)
	}
}

func (cs capturers) captureExecCommand(command string) {
	for _, c := range cs {
		c.CaptureExecCommand(command)
	}
}

func (cs capturers) captureExecStdin(stdin string) {
	for _, c := range cs {
		c.CaptureExecStdin(stdin)
	}
}

func (cs capturers) captureExecStdout(stdout string) {
	for _, c := range cs {
		c.CaptureExecStdout(stdout)
	}
}

func (cs capturers) captureExecStderr(stderr string) {
	for _, c := range cs {
		c.CaptureExecStderr(stderr)
	}
}

func (cs capturers) captureShellCommand(command string) {
	for _, c := range cs {
		c.CaptureShellCommand(command)
	}
}

func (cs capturers) captureShellStdin(stdin string) {
	for _, c := range cs {
		c.CaptureShellStdin(stdin)
	}
}

func (cs capturers) captureShellStdout(stdout string) {
	for _, c := range cs {
		c.CaptureShellStdout(stdout)
	}
}

func (cs capturers) captureShellStderr(stderr string) {
	for _, c := range cs {
		c.CaptureShellStderr(stderr)
	}
}

func (cs capturers) setCurrentIDs(ids IDs) {
	for _, c := range cs {
		c.SetCurrentIDs(ids)
	}
}
