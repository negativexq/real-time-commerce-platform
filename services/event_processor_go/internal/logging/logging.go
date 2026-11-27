// Package logging emits JSON lines shaped like the Python structlog output:
// {"event": ..., "level": "info", "timestamp": "...Z", ...fields}.
package logging

import (
	"log/slog"
	"os"
	"strings"
	"time"
)

// LevelCritical maps structlog's CRITICAL level.
const LevelCritical = slog.Level(12)

func parseLevel(name string) slog.Level {
	switch strings.ToUpper(name) {
	case "DEBUG":
		return slog.LevelDebug
	case "WARNING":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	case "CRITICAL":
		return LevelCritical
	}
	return slog.LevelInfo
}

// Configure installs the process-wide JSON logger at the given level.
func Configure(level string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLevel(level),
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) > 0 {
				return a
			}
			switch a.Key {
			case slog.MessageKey:
				a.Key = "event"
			case slog.TimeKey:
				a.Key = "timestamp"
				a.Value = slog.StringValue(a.Value.Time().UTC().Format(time.RFC3339Nano))
			case slog.LevelKey:
				lvl := a.Value.Any().(slog.Level)
				name := "info"
				switch {
				case lvl >= LevelCritical:
					name = "critical"
				case lvl >= slog.LevelError:
					name = "error"
				case lvl >= slog.LevelWarn:
					name = "warning"
				case lvl < slog.LevelInfo:
					name = "debug"
				}
				a.Value = slog.StringValue(name)
			}
			return a
		},
	})
	logger := slog.New(handler).With("logger", "event_processor")
	slog.SetDefault(logger)
	return logger
}
