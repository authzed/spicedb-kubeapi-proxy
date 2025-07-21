package distributedtx

import (
	"context"
	"log/slog"

	"k8s.io/klog/v2"
)

type klogToSlogAdapter struct {
	klogLogger klog.Logger
}

func (a *klogToSlogAdapter) Enabled(ctx context.Context, level slog.Level) bool {
	return a.klogLogger.Enabled()
}

func (a *klogToSlogAdapter) Handle(ctx context.Context, record slog.Record) error {
	args := make([]any, 0, record.NumAttrs()*2)
	record.Attrs(func(attr slog.Attr) bool {
		args = append(args, attr.Key, attr.Value.Any())
		return true
	})

	switch record.Level {
	case slog.LevelDebug:
		a.klogLogger.V(4).Info(record.Message, args...)
	case slog.LevelInfo:
		a.klogLogger.Info(record.Message, args...)
	case slog.LevelWarn:
		a.klogLogger.Info(record.Message, args...)
	case slog.LevelError:
		a.klogLogger.Error(nil, record.Message, args...)
	default:
		a.klogLogger.Info(record.Message, args...)
	}

	return nil
}

func (a *klogToSlogAdapter) WithAttrs(attrs []slog.Attr) slog.Handler {
	args := make([]any, 0, len(attrs))
	for _, attr := range attrs {
		args = append(args, attr.Key, attr.Value.Any())
	}

	return newKlogToSlogAdapter(a.klogLogger.WithValues(args...))
}

func (a *klogToSlogAdapter) WithGroup(name string) slog.Handler {
	return newKlogToSlogAdapter(a.klogLogger.WithName(name))
}

func newKlogToSlogAdapter(klogLogger klog.Logger) slog.Handler {
	return &klogToSlogAdapter{klogLogger: klogLogger}
}
