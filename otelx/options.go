package otelx

import (
	"context"
	"log"

	"github.com/ValerySidorin/corex/errx"
	"go.opentelemetry.io/otel/sdk/resource"
	"google.golang.org/grpc"
)

type Option func(*Otel)

func WithCtx(ctx context.Context) Option {
	return func(o *Otel) {
		o.ctx = ctx
	}
}

func WithResource(res *resource.Resource) Option {
	return func(o *Otel) {
		var err error
		o.resource, err = resource.Merge(o.resource, res)
		if err != nil {
			log.Fatal(errx.Wrap("merge resource", err))
		}
	}
}

func WithTracingOtlpCollector(target string, dialOptions ...grpc.DialOption) Option {
	return func(o *Otel) {
		o.otelTracingOtlpCollectorEnabled = true
		o.otelTracingOtlpCollectorTarget = target
		o.otelTracingOtlpCollectorDialOpts = append(
			o.otelTracingOtlpCollectorDialOpts, dialOptions...)
	}
}

func WithTracingStdout() Option {
	return func(o *Otel) {
		o.otelTracingStdoutEnabled = true
	}
}

func WithMetricsOtlpCollector(target string, dialOptions ...grpc.DialOption) Option {
	return func(o *Otel) {
		o.otelMetricsOtlpCollectorEnabled = true
		o.otelMetricsOtlpCollectorTarget = target
		o.otelMetricsOtlpCollectorDialOpts = append(
			o.otelMetricsOtlpCollectorDialOpts, dialOptions...)
	}
}

func WithMetricsPrometheus() Option {
	return func(o *Otel) {
		o.otelMetricsPrometheusEnabled = true
	}
}
