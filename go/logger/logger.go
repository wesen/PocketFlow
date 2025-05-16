package logger

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

// Global logger instance
var log zerolog.Logger

// Initialize the logger with default settings
func init() {
	// Set global logging level to debug
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	// Configure the logger
	output := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
		NoColor:    false,
	}

	// Customize event formatting
	output.FormatLevel = func(i interface{}) string {
		return fmt.Sprintf("%-7s", fmt.Sprintf("[%s]", i))
	}

	output.FormatMessage = func(i interface{}) string {
		return fmt.Sprintf("%s", i)
	}

	output.FormatFieldName = func(i interface{}) string {
		return fmt.Sprintf("%s=", i)
	}

	output.FormatFieldValue = func(i interface{}) string {
		return fmt.Sprintf("%s", i)
	}

	// Create the logger
	log = zerolog.New(output).With().Timestamp().Caller().Logger()
}

// Get returns the global logger instance
func Get() zerolog.Logger {
	return log
}

// For creates a contextualized logger with the specified component field
func For(component string) zerolog.Logger {
	return log.With().Str("component", component).Logger()
}

// WithField creates a logger with an additional field
func WithField(key string, value interface{}) zerolog.Logger {
	return log.With().Interface(key, value).Logger()
}

// WithFields creates a logger with additional fields
func WithFields(fields map[string]interface{}) zerolog.Logger {
	ctx := log.With()
	for k, v := range fields {
		ctx = ctx.Interface(k, v)
	}
	return ctx.Logger()
}

// SetOutput changes the output of the global logger
func SetOutput(w io.Writer) {
	log = zerolog.New(w).With().Timestamp().Caller().Logger()
}

// SetLevel sets the global logging level
func SetLevel(level zerolog.Level) {
	zerolog.SetGlobalLevel(level)
}