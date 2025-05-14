package example

import (
	"context"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"net/http"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/kelub/goose/pkg/prom"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type grpcType string

const (
	Unary        grpcType = "unary"
	ClientStream grpcType = "client_stream"
	ServerStream grpcType = "server_stream"
	BidiStream   grpcType = "bidi_stream"
)

var (
	// serverStartedCounter is a counter metric that tracks the total number of RPCs started on the server.
	serverStartedCounter = prom.NewPromVec("samoyed").
				Counter("grpc_server_started_total",
			"Total number of RPCs started on the server.",
			[]string{"grpc_type", "grpc_service", "grpc_method", "origin", "referrer"})
	// serverHandledCounter is a counter metric that tracks the total number of RPCs completed on the server.
	serverHandledCounter = prom.NewPromVec("samoyed").
				Counter("grpc_server_handled_total",
			"Total number of RPCs completed on the server, regardless of success or failure.",
			[]string{"grpc_type", "grpc_service", "grpc_method", "origin", "referrer", "grpc_code"}).
		Histogram("grpc_server_handling_seconds",
			"Histogram of response latency (seconds) of gRPC that had been application-level handled by the server.",
			[]string{"grpc_type", "grpc_service", "grpc_method", "origin", "referrer", "grpc_code"},
			prometheus.ExponentialBuckets(0.001, 2.0, 17))
)

// StartMetricServer starts a Prometheus metrics server on port 8080.
// use prom must init this function first
func StartMetricServer() {
	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.WithError(err).Error("start metric server failed")
	}
}

// UnaryPromInterceptor is a unary server interceptor that adds Prometheus metrics to the server.
func UnaryPromInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if _, ok := info.Server.(grpc_health_v1.HealthServer); ok {
			return handler(ctx, req)
		}

		var (
			service, method, origin, referrer, grpcCode string
			startTime                                   time.Time
		)
		md := metautils.ExtractIncoming(ctx)
		origin = md.Get("origin")
		referrer = md.Get("referrer-service")
		service, method = splitMethodName(info.FullMethod)

		// Increment the serverStartedCounter for the unary RPC.
		serverStartedCounter.Inc(string(Unary), service, method, origin, referrer)

		startTime = time.Now()
		resp, err := handler(ctx, req)
		grpcCode = status.Code(err).String()

		// Increment the serverHandledCounter for the unary RPC.
		serverHandledCounter.Inc(string(Unary), service, method, origin, referrer, grpcCode)
		// Observe the serverHandledCounter for the unary RPC.
		serverHandledCounter.HandleTime(startTime, string(Unary), service, method, origin, referrer, grpcCode)
		return resp, err
	}
}

// splitMethodName splits the full method name into service and method.
func splitMethodName(fullMethodName string) (string, string) {
	fullMethodName = strings.TrimPrefix(fullMethodName, "/") // remove leading slash
	if i := strings.Index(fullMethodName, "/"); i >= 0 {
		return fullMethodName[:i], fullMethodName[i+1:]
	}
	return "unknown", "unknown"
}
