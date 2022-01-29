package connector

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

type replyHandlerWrapper struct {
	ReplyHandler
	sendAt    int64
	expiresAt int64
}

func (r *replyHandlerWrapper) Reply(data []byte) {
	r.ReplyHandler(data, nil)
}

func (r *replyHandlerWrapper) ReplyError(err error) {
	r.ReplyHandler(nil, err)
}

type KafkaConnector struct {
	readers            map[string]*kafka.Reader
	writer             *kafka.Writer
	replyHandlers      map[string]*replyHandlerWrapper
	handlerTTLNano     int64
	serializerRegistry SerializersRegistry
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
		expiresAt:    now + k.handlerTTLNano,
	}

	k.replyHandlers[request.ID] = replyWrapper

	if err := k.Send(request, opts); err != nil {
		delete(k.replyHandlers, request.ID)
		replyWrapper.ReplyError(err)
		return err
	}

	return nil
}

func (k *KafkaConnector) SetUp() {
	now := time.Now()

	for _, reader := range k.readers {
		go k.consumeFromReader(reader, now)
	}
}

func (k *KafkaConnector) consumeFromReader(reader *kafka.Reader, startTime time.Time) {
	reader.SetOffsetAt(context.Background(), startTime)
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
	go handler.Reply(message.Value)
}

func (k *KafkaConnector) Close() error {
	return nil
}
