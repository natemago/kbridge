package kbridge

import (
	"errors"
	"fmt"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

type ServerConfig struct {
	HTTPConfig *HTTPConfig `json:"http" yaml:"http" mapstructure:"http"`
}

type HTTPConfig struct {
	Host string `json:"host" yaml:"host" mapstructure:"host"`
	Port int    `json:"port" yaml:"port" mapstructure:"port"`
}

type KafkaConfig struct {
	KafkaURL string `json:"kafkaUrl" yaml:"kafkaUrl" mapstructure:"kafkaUrl"`
}

type EndpointDefinition struct {
	IsGRPC     bool   `json:"grpc" yaml:"grpc" mapstructure:"grpc"`
	Path       string `json:"path" yaml:"path" mapstructure:"path"`
	HTTPMethod string `json:"method" yaml:"method" mapstructure:"method"`
	Topic      string `json:"topic" yaml:"topic" mapstructure:"topic"`
	ReplyTopic string `json:"reply-topic" yaml:"reply-topic" mapstructure:"reply-topic"`
}

type Config struct {
	Server    *ServerConfig         `json:"server" yaml:"server" mapstructure:"server"`
	Kafka     *KafkaConfig          `json:"kafka" yaml:"kafka" mapstructure:"kafka"`
	Endpoints []*EndpointDefinition `json:"endpoints" yaml:"endpoints" mapstructure:"endpoints"`
}

func initConfigParams() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")

	viper.AddConfigPath(fmt.Sprintf("/etc/%s", AppName))
	viper.AddConfigPath(fmt.Sprintf("$HOME/.%s", AppName))
	viper.AddConfigPath(fmt.Sprintf("$HOME/.config/%s", AppName))
	viper.AddConfigPath(".")
}

func LoadConfig() (*Config, error) {

	initConfigParams()

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Error().Str("error", err.Error()).Msg("Config file not found")
			return nil, err
		}
		log.Error().Str("error", err.Error()).Msg("Failed to load config")
		return nil, err
	}

	config := &Config{}
	if err := viper.Unmarshal(config); err != nil {
		log.Error().Str("error", err.Error()).Msg("Failed to parse config")
		return nil, err
	}
	return config, nil
}

func LoadConfigFromFile(configFile string) (*Config, error) {

	viper.SetConfigType("yaml")

	if _, err := os.Stat(configFile); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log.Error().Str("configFile", configFile).Msg("File does not exist")
			return nil, err
		}
		log.Error().Str("configFile", configFile).Msg(err.Error())
		return nil, err
	}
	confFileReader, err := os.Open(configFile)
	if err != nil {
		log.Error().Str("configFile", configFile).Msg(err.Error())
		return nil, err
	}
	if err := viper.ReadConfig(confFileReader); err != nil {
		log.Error().Str("error", err.Error()).Msg("Failed to read config")
		return nil, err
	}
	config := &Config{}
	if err := viper.Unmarshal(config); err != nil {
		log.Error().Str("error", err.Error()).Msg("Failed to parse config")
		return nil, err
	}
	return config, nil
}
