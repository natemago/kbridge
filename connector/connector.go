package connector

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/rs/zerolog/log"
)

type Message struct {
	ID        string
	Type      string
	Port      string
	Variables map[string]string
	Headers   map[string]string
	Payload   []byte
}

type SendOptions struct {
	Topic       string
	Partition   int
	Passthrough bool
}

type ReplyHandler func(reply []byte, err error)

type Connector interface {
	Send(message *Message, opts *SendOptions) error
	RequestReply(request *Message, opts *SendOptions, then ReplyHandler) error
	Close() error
}

func (m *Message) Validate() error {
	if m.ID == "" {
		return fmt.Errorf("missing message ID")
	}

	return nil
}

func NewMessageID(prefix string, byteSize int) string {
	buffer := make([]byte, byteSize)
	n, err := rand.Read(buffer)
	if err != nil {
		log.Fatal().Str("error", err.Error()).Msgf("Failed to generate random message id: %s", err.Error())
		return ""
	}
	if n != byteSize {
		log.Fatal().Msgf("Failed to generate random mesage id. Expected %d bytes, but got %d random bytes instead.", byteSize, n)
		return ""
	}
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(buffer))
}

type MessageSerializer interface {
	Serialize(msg *Message) ([]byte, error)
}

type SerializersRegistry struct {
	serializers map[string]MessageSerializer
}

func (r *SerializersRegistry) Register(messageType string, serializer MessageSerializer) {
	r.serializers[messageType] = serializer
}

func (r *SerializersRegistry) GetMessageTypes() []string {
	serializers := []string{}
	for mt := range r.serializers {
		serializers = append(serializers, mt)
	}
	return serializers
}

func (r *SerializersRegistry) GetSerializer(messageType string) (MessageSerializer, error) {
	if serializer, ok := r.serializers[messageType]; ok {
		return serializer, nil
	}
	return nil, fmt.Errorf("no serializer for type: %s", messageType)
}
