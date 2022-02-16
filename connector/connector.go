package connector

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/rs/zerolog/log"
)

type Message struct {
	ID         string
	Type       string
	Port       string
	Path       string
	Variables  map[string]string
	Parameters map[string][]string
	Headers    map[string]string
	Payload    []byte
}

type SendOptions struct {
	Topic          string
	Partition      int
	ReplyTopic     string
	ReplyPartition int
	Passthrough    bool
}

type MessageHeaders map[string]interface{}

type ReplyHandler func(reply []byte, headers MessageHeaders, err error)

type Connector interface {
	Send(message *Message, opts *SendOptions) error
	RequestReply(request *Message, opts *SendOptions, then ReplyHandler) error
	Close() error
}

func (m *Message) Validate() error {
	if m.ID == "" {
		return ValidationError("missing message ID")
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
	return nil, ConfigurationError(fmt.Sprintf("no serializer for type: %s", messageType))
}

func NewSerializerRegistry() *SerializersRegistry {
	return &SerializersRegistry{
		serializers: map[string]MessageSerializer{},
	}
}

func (h MessageHeaders) GetString(key string) string {
	if value, ok := h[key]; ok {
		if strValue, ok := value.(string); ok {
			return strValue
		}
		if byteVal, ok := value.([]byte); ok {
			return string(byteVal)
		}
	}
	return ""
}

// ConnectorError error interface

type connectorError struct {
	errType string
	message string
}

func (c *connectorError) Error() string {
	return c.message
}

type ConnectorError func(message string) error

func ConnectorErrorType(errorType string) ConnectorError {
	return func(message string) error {
		return &connectorError{
			errType: errorType,
			message: message,
		}
	}
}

func IsErrorOfType(errType string, err error) bool {
	if connErr, ok := err.(*connectorError); ok {
		return connErr.errType == errType
	}
	return false
}

var TimeoutError = ConnectorErrorType("timeout")
var ValidationError = ConnectorErrorType("validation")
var ConfigurationError = ConnectorErrorType("config")
