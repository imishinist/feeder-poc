package cmd

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

type Logger struct {
	filename string
	file     *os.File

	options *slog.HandlerOptions
	slogger *slog.Logger
}

func NewLogger(filename string, options *slog.HandlerOptions) (*Logger, error) {
	logger := &Logger{
		filename: filename,
		options:  options,
	}
	if err := logger.Reload(); err != nil {
		return nil, err
	}
	return logger, nil
}

func (l *Logger) Info(msg string, args ...any) {
	l.slogger.Info(msg, args...)
}

func (l *Logger) Error(msg string, args ...any) {
	l.slogger.Error(msg, args...)
}

func (l *Logger) Close() {
	l.file.Close()
}

func (l *Logger) Reload() error {
	if l.file != nil {
		if err := l.file.Close(); err != nil {
			return err
		}
	}

	f, err := os.OpenFile(l.filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	l.file = f
	l.slogger = slog.New(slog.NewJSONHandler(f, l.options))
	return nil
}

// logCmd represents the log command
var logCmd = &cobra.Command{
	Use:  "log",
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		file := args[0]

		logger, err := NewLogger(file, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})
		if err != nil {
			return err
		}
		defer logger.Close()

		ctx, cancel := context.WithCancel(cmd.Context())
		defer cancel()

		ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
		defer stop()

		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGHUP)

		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-c:
					if err := logger.Reload(); err != nil {
						log.Println(err)
						cancel()
					}
					log.Println("Reopened log file")
				}
			}
		}()

	L:
		for {
			select {
			case <-ctx.Done():
				break L
			case <-time.After(1 * time.Second):
			}
			logger.Info("Hello, World!")
			logger.Error("Goodbye, World!")
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(logCmd)
}
