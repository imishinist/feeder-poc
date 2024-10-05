package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// consumeCmd represents the consume command
var consumeCmd = &cobra.Command{
	Use: "consume",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("consume called")
	},
}

func init() {
	rootCmd.AddCommand(consumeCmd)
}
