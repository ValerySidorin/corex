package otelx

import (
	"context"
	"errors"
	"sync"

	"github.com/ValerySidorin/corex/errx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
)

var (
	initOnce sync.Once
	o        *Otel

	initResourceOnce sync.Once
	otelResource     = resource.Default()
)

func GetResource() (*resource.Resource, error) {
	if o == nil {
		return nil, errors.New("otelx is not instantiated")
	}
	return otelResource, nil
}

type Otel struct {
	resource *resource.Resource

	otelTracingOtlpCollectorEnabled  bool
	otelTracingOtlpCollectorTarget   string
	otelTracingOtlpCollectorDialOpts []grpc.DialOption
	otelTracerProviderShutdown       func(context.Context) error

	otelTracingStdoutEnabled     bool
	otelMetricsPrometheusEnabled bool

	otelMetricsOtlpCollectorEnabled    bool
	otelMetricsOtlpCollectorTarget     string
	otelMetricsOtlpCollectorDialOpts   []grpc.DialOption
	otelCollectorMeterProviderShutdown func(context.Context) error

	ctx context.Context
}

func InitResource(ctx context.Context, options ...resource.Option) *resource.Resource {
	initResourceOnce.Do(func() {
		extraResources, _ := resource.New(ctx, options...)
		otelResource, _ = resource.Merge(
			resource.Default(),
			extraResources,
		)
	})

	return otelResource
}

func Init(options ...Option) error {
	var initErr error

	initOnce.Do(func() {
		o = &Otel{}
		o.resource = resource.Default()

		for _, opt := range options {
			opt(o)
		}

		if o.ctx == nil {
			o.ctx = context.Background()
		}

		var (
			conn *grpc.ClientConn
			err  error
		)

		if o.otelTracingOtlpCollectorEnabled {
			conn, err = initCollectorConn(o.otelTracingOtlpCollectorTarget,
				o.otelTracingOtlpCollectorDialOpts...)
			if err != nil {
				initErr = errx.Wrap("init otel tracing conn", err)
				return
			}

			o.otelTracerProviderShutdown, err = initCollectorTracerProvider(
				o.ctx, conn, o.resource)
			if err != nil {
				initErr = errx.Wrap("init otel tracer provider", err)
				return
			}
		}

		if o.otelMetricsOtlpCollectorEnabled {
			otelMetricsCollectorConn := conn
			if o.otelMetricsOtlpCollectorTarget != o.otelTracingOtlpCollectorTarget {
				otelMetricsCollectorConn, err = initCollectorConn(
					o.otelMetricsOtlpCollectorTarget,
					o.otelMetricsOtlpCollectorDialOpts...)
				if err != nil {
					initErr = errx.Wrap("init otel metrics conn", err)
				}
			}

			o.otelCollectorMeterProviderShutdown, err = initCollectorMeterProvider(
				o.ctx, otelMetricsCollectorConn, o.resource)
			if err != nil {
				initErr = errx.Wrap("init otel collector meter provider", err)
			}
		}

		if o.otelTracingStdoutEnabled {
			if err := initStdoutTracerProvider(o.resource); err != nil {
				initErr = errx.Wrap("init otel stdout tracer provider", err)
			}
		}

		if o.otelMetricsPrometheusEnabled {
			if err := initPrometheusMeterProvider(o.resource); err != nil {
				initErr = errx.Wrap("init otel prometheus meter provider", err)
			}
		}
	})

	return initErr
}

func Enabled() bool {
	return o != nil
}

func initCollectorConn(
	target string,
	opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(target, opts...)
	return conn, errx.Wrap("init grpc client", err)
}

func initCollectorTracerProvider(
	ctx context.Context,
	conn *grpc.ClientConn,
	res *resource.Resource) (func(context.Context) error, error) {
	traceExporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, errx.Wrap("init grpc trace exporter", err)
	}

	bsp := trace.NewBatchSpanProcessor(traceExporter)
	tracerProvider := trace.NewTracerProvider(
		trace.WithSampler(trace.AlwaysSample()),
		trace.WithResource(res),
		trace.WithSpanProcessor(bsp),
	)
	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tracerProvider.Shutdown, nil
}

func initStdoutTracerProvider(res *resource.Resource) error {
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return errx.Wrap("init stdout trace exporter", err)
	}
	tp := trace.NewTracerProvider(
		trace.WithSampler(trace.AlwaysSample()),
		trace.WithSyncer(exporter),
		trace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	return nil
}

func initCollectorMeterProvider(
	ctx context.Context,
	conn *grpc.ClientConn,
	res *resource.Resource) (func(context.Context) error, error) {
	metricExporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, errx.Wrap("init metric exporter", err)
	}

	meterProvider := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(metricExporter)),
		metric.WithResource(res),
	)
	otel.SetMeterProvider(meterProvider)
	return meterProvider.Shutdown, nil
}

func initPrometheusMeterProvider(res *resource.Resource) error {
	metricExporter, err := prometheus.New()
	if err != nil {
		return errx.Wrap("init prometheus metric exporter", err)
	}
	meterProvider := metric.NewMeterProvider(
		metric.WithReader(metricExporter),
		metric.WithResource(res),
	)
	otel.SetMeterProvider(meterProvider)
	return nil
}
