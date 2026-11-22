package main

import (
	"clientapi/internal/platform"
	"context"
	"errors"
	"io"
	"net"
	"syscall"
)

// failureClass names why an upstream call got no usable answer, so metrics show the cause even
// after the logs that carried the error text have rotated away.
func failureClass(err error) string {
	var network net.Error
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &network) && network.Timeout():
		return "timeout"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "refused"
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE):
		return "reset"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "eof"
	default:
		return "other"
	}
}

// countUpstreamFailure records one failed upstream call as
// app_upstream_failures_total_<service>_<class>; class is a failureClass or "status_5xx".
func countUpstreamFailure(service, class string) {
	platform.Add("upstream_failures_total_"+service+"_"+class, 1)
}
