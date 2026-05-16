package rpc

import (
	"fmt"
	"strings"
)

// Subject namespaces.
//
// The original ("legacy") namespace shares a single subject between unary
// and streaming RPCs, which forces the server to peek at the request type
// to dispatch and prevents structurally separating "safe queue-balance for
// unary" from "point-to-point for streaming". See SIDECAR.md §2.
//
// The "modern" namespace introduces two prefixes so the safety guarantee
// is structural:
//
//   unary:    nrpc.unary.<svcid>.<package>.<Service>.<Method>
//   streaming: nrpc.stream.<svcid>.<nid>.<package>.<Service>.<Method>
//
// Both layouts are supported; the legacy one is used by the direct
// pkg/rpc.Client/Server API for backward compatibility, the modern one
// is used by the sidecar.
const (
	subjectPrefixLegacy  = "nrpc"
	subjectPrefixUnary   = "nrpc.unary"
	subjectPrefixStream  = "nrpc.stream"
)

// methodSuffix converts a gRPC method path ("/pkg.Service/Method") to its
// NATS subject suffix (".pkg.Service.Method"). Inverse of grpc-go's
// canonical method path format.
func methodSuffix(fullMethod string) string {
	return strings.ReplaceAll(fullMethod, "/", ".")
}

// legacySubject builds the subject used by the direct pkg/rpc API. svcid
// may be empty, in which case the leading "nrpc.<svcid>" collapses to just
// "nrpc". Matches the pre-existing inlined builder in Client.invoker /
// Server.RegisterService — extracted so the sidecar's modern builders can
// share the suffix logic.
func legacySubject(svcid, fullMethod string) string {
	prefix := subjectPrefixLegacy
	if svcid != "" {
		prefix = fmt.Sprintf("%s.%s", subjectPrefixLegacy, svcid)
	}
	return prefix + methodSuffix(fullMethod)
}

// modernUnarySubject builds the unary subject in the namespace the
// sidecar uses: nrpc.unary.<svcid>.<package>.<Service>.<Method>.
// svcid is required.
func modernUnarySubject(svcid, fullMethod string) string {
	return fmt.Sprintf("%s.%s%s", subjectPrefixUnary, svcid, methodSuffix(fullMethod))
}

// modernStreamSubject builds the streaming subject in the modern
// namespace: nrpc.stream.<svcid>.<nid>.<package>.<Service>.<Method>.
// Both svcid and nid are required — nid pins the call to a specific
// server replica so multi-frame streams stay coherent.
func modernStreamSubject(svcid, nid, fullMethod string) string {
	return fmt.Sprintf("%s.%s.%s%s", subjectPrefixStream, svcid, nid, methodSuffix(fullMethod))
}

// modernUnaryQueueGroup is the queue group name backend replicas use when
// subscribing to the modern unary subject. All replicas sharing svcid
// share the queue group, so NATS load-balances each call to exactly one.
func modernUnaryQueueGroup(svcid, fullService string) string {
	return fmt.Sprintf("u:%s:%s", svcid, fullService)
}
