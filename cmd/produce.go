package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// produceCmd represents the producer command
var produceCmd = &cobra.Command{
	Use: "produce",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("produce called")
	},
}

func init() {
	rootCmd.AddCommand(produceCmd)
}
