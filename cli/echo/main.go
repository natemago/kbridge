package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/natemago/kbridge/connector"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "echo-client",
	Short: "Kafka echo client",
	Run:   RunEchoClient,
}

type Options struct {
	KafkaURL       string
	Topic          string
	Partition      int
	ReplyTopic     string
	ReplyPartition int
}

var ProgramOptions = Options{}

func init() {
	rootCmd.Flags().StringVar(&ProgramOptions.KafkaURL, "url", "", "Kafka Broker URL.")
	rootCmd.Flags().StringVar(&ProgramOptions.Topic, "topic", "", "Listen on topic.")
	rootCmd.Flags().StringVar(&ProgramOptions.ReplyTopic, "reply", "", "Reply on topic.")
	rootCmd.Flags().IntVar(&ProgramOptions.Partition, "partition", 0, "Incoming topic partition.")
	rootCmd.Flags().IntVar(&ProgramOptions.Partition, "reply-partition", 0, "Reply topic partition.")
}

func RunEchoClient(cmd *cobra.Command, args []string) {

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{ProgramOptions.KafkaURL},
		Topic:     ProgramOptions.Topic,
		Partition: ProgramOptions.Partition,
	})

	reader.SetOffsetAt(context.Background(), time.Now())

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:   []string{ProgramOptions.KafkaURL},
		BatchSize: 1,
	})

	go func() {
		for {
			message, err := reader.ReadMessage(context.Background())
			if err != nil {
				log.Error().Msgf("Failed to read message from Kafka: %s", err.Error())
				continue
			}
			log.Info().Msgf("%s => %s", string(message.Key), string(message.Value))

			responseValue := message.Value
			parsedValue := &connector.Message{}
			if err := json.Unmarshal(message.Value, parsedValue); err == nil {
				responseValue = parsedValue.Payload
			}

			if err := writer.WriteMessages(context.Background(), kafka.Message{
				Key:       message.Key,
				Value:     responseValue,
				Topic:     ProgramOptions.ReplyTopic,
				Partition: ProgramOptions.ReplyPartition,
			}); err != nil {
				log.Error().Msgf("Failed to reply echo message to Kafka: %s", err.Error())
			}
			log.Info().Msg("Reply send.")
		}
	}()

	sigs := make(chan os.Signal, 1)
	done := make(chan bool)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		writer.Close()
		reader.Close()
		done <- true
	}()

	<-done

}

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	rootCmd.Execute()
}
