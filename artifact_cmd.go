package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"go-scheduler/transfer"
)

func artifactTransferCommand() *cobra.Command {
	var encoded string
	cmd := &cobra.Command{Use: "artifact-transfer", Short: "Transfer one reviewed artifact on its data worker", RunE: func(cmd *cobra.Command, args []string) error {
		raw, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil || len(raw) > 65536 {
			return fmt.Errorf("invalid transfer request")
		}
		var request transfer.Command
		if err = json.Unmarshal(raw, &request); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
		defer cancel()
		receipt, err := transfer.ExecuteCommand(ctx, request)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(receipt)
	}}
	cmd.Flags().StringVar(&encoded, "request-base64", "", "Encoded reviewed transfer command")
	_ = cmd.MarkFlagRequired("request-base64")
	return cmd
}
