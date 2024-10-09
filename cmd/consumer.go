package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/imishinist/feeder-poc/config"
)

type ConsumerWorker struct {
	config *config.ConsumerWorker
}

// consumerCmd represents the consumer command
var consumerCmd = &cobra.Command{
	Use: "consumer",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("consumer called")
	},
}

func init() {
	rootCmd.AddCommand(consumerCmd)
}
