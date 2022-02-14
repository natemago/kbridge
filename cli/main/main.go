package main

import (
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/natemago/kbridge"
	"github.com/natemago/kbridge/connector"
	"github.com/natemago/kbridge/server"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "kbridge",
	Short: "kbridge is a HTTP and gRPC bridge for Kafka",
	Run:   RunKBridge,
}

type Options struct {
	ConfigFile string
}

var ProgramOptions = &Options{}

func init() {
	rootCmd.Flags().StringVar(&ProgramOptions.ConfigFile, "config", "", "Explicitly set configuration file. ")
}

func loadConfig() *kbridge.Config {
	if ProgramOptions.ConfigFile == "" {
		config, err := kbridge.LoadConfig()
		if err != nil {
			panic("Failed to load config")
		}
		return config
	}

	config, err := kbridge.LoadConfigFromFile(ProgramOptions.ConfigFile)
	if err != nil {
		panic("Failed to load config")
	}
	return config
}

func RunKBridge(cmd *cobra.Command, args []string) {
	config := loadConfig()
	conn, err := connector.CreateKafkaConnector(config)
	if err != nil {
		log.Fatal().Str("error", err.Error()).Msgf("Failed to create connector: %s", err.Error())
	}

	httpServer := server.NewHTTPServer(config, conn)

	quit := make(chan os.Signal, 1)

	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := httpServer.Run(); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				log.Error().Str("error", err.Error()).Msgf("Server did not shut down properly: %s", err.Error())
			}
		}
	}()

	<-quit
	if err := httpServer.Shutdown(10 * time.Second); err != nil {
		log.Error().Str("error", err.Error()).Msgf("Server exited with error: %s", err.Error())
	}
}

func main() {
	rootCmd.Execute()
}
