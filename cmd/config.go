package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

type Config struct {
	HomeDir     string `yaml:"home_dir"`
	Concurrency int    `yaml:"concurrency"`
}

var (
	testConfig = NewReloadable[Config](&Config{
		HomeDir:     "/home/user",
		Concurrency: 10,
	}, syscall.SIGHUP)
)

func PrintConfig(conf *Config) {
	fmt.Println("config:")
	fmt.Println("  HomeDir:", conf.HomeDir)
	fmt.Println("  Concurrency:", conf.Concurrency)
}

func LoadConfig(file string, conf *Config) error {
	// Load config from file
	f, err := os.Open(file)
	if err != nil {
		return fmt.Errorf("open config file error: %w", err)
	}
	defer f.Close()

	if err := yaml.NewDecoder(f).Decode(conf); err != nil {
		return fmt.Errorf("decode config error: %w", err)
	}

	return nil
}

type Reloadable[T any] struct {
	Value *T

	signal chan os.Signal
}

func NewReloadable[T any](value *T, sig ...os.Signal) *Reloadable[T] {
	c := make(chan os.Signal, 1)
	if len(sig) == 0 {
		sig = []os.Signal{syscall.SIGHUP}
	}

	signal.Notify(c, sig...)
	return &Reloadable[T]{
		Value:  value,
		signal: c,
	}
}

func (r *Reloadable[T]) Watch(ctx context.Context, do func(T) T) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.signal:
			tmp := do(*r.Value)
			r.Value = &tmp
		}
	}
}

// configCmd represents the config command
var configCmd = &cobra.Command{
	Use:  "config",
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		confFile := args[0]
		if err := LoadConfig(confFile, testConfig.Value); err != nil {
			return err
		}

		ctx, cancel := context.WithCancel(cmd.Context())
		defer cancel()

		ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		c := make(chan os.Signal, 1)
		// same signal
		signal.Notify(c, syscall.SIGHUP)
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-c:
				// for loop じゃないので、一度だけしか表示されない
				fmt.Println("SIGHUP received")
			}
		}()

		// conf reload
		go testConfig.Watch(ctx, func(current Config) Config {
			var conf Config

			fmt.Println("Reloading config...")
			if err := LoadConfig(confFile, &conf); err != nil {
				fmt.Println("Reload config error:", err)
				PrintConfig(&current)
				return current
			}
			PrintConfig(&conf)
			return conf
		})

		for {
			select {
			case <-ctx.Done():
				fmt.Println(ctx.Err())
				return nil
			default:
			}

			time.Sleep(1 * time.Second)
		}
	},
}

func init() {
	rootCmd.AddCommand(configCmd)
}
