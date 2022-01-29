package connector

import (
	"encoding/json"

	"gopkg.in/yaml.v2"
)

type JSONSerializer struct{}

func (js *JSONSerializer) Serialize(msg *Message) ([]byte, error) {
	return json.Marshal(msg)
}

type YAMLSerializer struct{}

func (ys *YAMLSerializer) Serialize(msg *Message) ([]byte, error) {
	return yaml.Marshal(msg)
}
