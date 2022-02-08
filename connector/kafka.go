package connector

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/natemago/kbridge"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

type replyHandlerWrapper struct {
	ReplyHandler
	sendAt    int64
	expiresAt int64
}

func (r *replyHandlerWrapper) Reply(data []byte, headers MessageHeaders) {
	r.ReplyHandler(data, headers, nil)
}

func (r *replyHandlerWrapper) ReplyError(err error) {
	r.ReplyHandler(nil, nil, err)
}

type KafkaConnector struct {
	readers            map[string]*kafka.Reader
	writer             *kafka.Writer
	replyHandlers      map[string]*replyHandlerWrapper
	handlerTTL         time.Duration
	serializerRegistry *SerializersRegistry
	started            bool
	closeMux           sync.Mutex
}

func (k *KafkaConnector) Send(message *Message, opts *SendOptions) error {
	if err := message.Validate(); err != nil {
		return err
	}

	serializer, err := k.serializerRegistry.GetSerializer(message.Type)
	if err != nil {
		return err
	}

	payload, err := serializer.Serialize(message)
	if err != nil {
		return err
	}

	return k.writer.WriteMessages(context.Background(), kafka.Message{
		Key:       []byte(message.ID),
		Topic:     opts.Topic,
		Partition: opts.Partition,
		Value:     payload,
	})
}

func (k *KafkaConnector) RequestReply(request *Message, opts *SendOptions, then ReplyHandler) error {
	if err := request.Validate(); err != nil {
		return err
	}

	now := time.Now().UnixNano()

	replyWrapper := &replyHandlerWrapper{
		ReplyHandler: then,
		sendAt:       now,
		expiresAt:    now + int64(k.handlerTTL),
	}

	k.replyHandlers[request.ID] = replyWrapper

	if err := k.Send(request, opts); err != nil {
		delete(k.replyHandlers, request.ID)
		replyWrapper.ReplyError(err)
		return err
	}

	return nil
}

func (k *KafkaConnector) maintenance() {
	expired := []string{}
	now := time.Now().UnixNano()
	for replyID, handler := range k.replyHandlers {
		if handler.expiresAt <= now {
			expired = append(expired, replyID)
		}
	}

	for _, replyID := range expired {
		handler := k.replyHandlers[replyID]
		delete(k.replyHandlers, replyID)
		handler.ReplyError(fmt.Errorf("timeout"))
	}
}

func (k *KafkaConnector) startMaintenanceLoop() {
	for {
		if !k.started {
			break
		}
		time.Sleep(100 * time.Millisecond)
		k.maintenance()
	}
}

func (k *KafkaConnector) SetUp() {
	now := time.Now()

	for _, reader := range k.readers {
		go k.consumeFromReader(reader, now)
	}

	k.closeMux.Lock()
	k.started = true
	k.closeMux.Unlock()
	go k.startMaintenanceLoop()
}

func (k *KafkaConnector) consumeFromReader(reader *kafka.Reader, startTime time.Time) {
	if err := reader.SetOffsetAt(context.Background(), startTime); err != nil {
		log.Fatal().Str("error", err.Error()).Msgf("Failed to set read offset for reader: %s", err.Error())
		return
	}

	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			break
		}
		go k.handleMessage(message)
	}
}

func (k *KafkaConnector) handleMessage(message kafka.Message) {
	handler, ok := k.replyHandlers[string(message.Key)]
	if !ok {
		return
	}

	headers := MessageHeaders{}

	if message.Headers != nil {
		for _, header := range message.Headers {
			headers[header.Key] = header.Value
		}
	}

	delete(k.replyHandlers, string(message.Key))

	go handler.Reply(message.Value, headers)
}

func (k *KafkaConnector) init(config *kbridge.Config) error {
	if err := k.setupReaders(config); err != nil {
		defer k.Close()
		return err
	}

	k.setupWriter(config)
	return nil
}

func (k *KafkaConnector) setupReaders(config *kbridge.Config) error {
	kconf := config.Kafka

	for _, endpoint := range config.Endpoints {

		readTopic := endpoint.Kafka.ReplyTopic
		readPartition := endpoint.Kafka.ReplyPartition

		if readTopic == "" {
			readTopic = fmt.Sprintf("%s-reply", endpoint.Kafka.Topic)
		}

		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{kconf.KafkaURL},
			Topic:     readTopic,
			Partition: readPartition,
		})

		k.readers[readTopic] = reader
		log.Info().Msgf("Reading from topic %s (partition %d)", readTopic, readPartition)
	}

	return nil
}

func (k *KafkaConnector) setupWriter(config *kbridge.Config) {
	k.writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{config.Kafka.KafkaURL},
		BatchSize:    config.Kafka.BatchSize,
		BatchTimeout: time.Duration(config.Kafka.BatchTimeout) * time.Millisecond,
	})
}

func (k *KafkaConnector) Close() error {
	k.closeMux.Lock()
	k.started = true
	k.closeMux.Unlock()

	errMessages := []string{}

	if k.writer != nil {
		if err := k.writer.Close(); err != nil {
			errMessages = append(errMessages, fmt.Sprintf("Failed to close Kafka writer: %s", err.Error()))
		}
	}

	if k.readers != nil {
		for topic, reader := range k.readers {
			if err := reader.Close(); err != nil {
				errMessages = append(errMessages, fmt.Sprintf("Failed to close Kafka reader for topic '%s': %s", topic, err.Error()))
			}
		}
		k.readers = nil
	}

	if k.replyHandlers != nil {
		k.replyHandlers = nil
	}

	if len(errMessages) > 0 {
		return fmt.Errorf("Close failed. Errors:\n%s", strings.Join(errMessages, "\n"))
	}

	return nil
}

func CreateKafkaConnector(config *kbridge.Config) (Connector, error) {

	serializerRegistry := NewSerializerRegistry()

	serializerRegistry.Register("json", &JSONSerializer{})
	serializerRegistry.Register("yaml", &YAMLSerializer{})

	conn := &KafkaConnector{
		readers:            make(map[string]*kafka.Reader),
		replyHandlers:      make(map[string]*replyHandlerWrapper),
		handlerTTL:         30 * time.Second,
		serializerRegistry: serializerRegistry,
	}

	if err := conn.init(config); err != nil {
		return nil, err
	}

	conn.SetUp()

	return conn, nil
}
