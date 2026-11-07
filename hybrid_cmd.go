package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/spf13/cobra"
	"go-scheduler/hybrid"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func hybridWorkersCommand() *cobra.Command {
	var configPath, queues string
	cmd := &cobra.Command{Use: "hybrid-workers", Short: "Serve explicitly selected hybrid/AWS worker queues", RunE: func(cmd *cobra.Command, args []string) error {
		cfg, e := hybrid.LoadConfig(configPath)
		if e != nil {
			return e
		}
		if queues == "" {
			return fmt.Errorf("--queues is required")
		}
		c, e := client.Dial(temporalClientOptions(slog.Default()))
		if e != nil {
			return e
		}
		defer c.Close()
		workers, e := hybrid.StartWorkers(context.Background(), c, cfg, strings.Split(queues, ","))
		if e != nil {
			return e
		}
		defer func() {
			for _, w := range workers {
				w.Stop()
			}
		}()
		<-worker.InterruptCh()
		return nil
	}}
	cmd.Flags().StringVar(&configPath, "config", "", "Immutable worker policy and site configuration")
	cmd.Flags().StringVar(&queues, "queues", "", "Comma-separated queues served by this host")
	_ = cmd.MarkFlagRequired("config")
	return cmd
}
