package server

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/natemago/kbridge"
	"github.com/natemago/kbridge/connector"
	"github.com/rs/zerolog/log"
)

type HTTPServer struct {
	Config         *kbridge.Config
	kafkaConnector connector.Connector
}

type ErrorMessage struct {
	Status int    `json:"status"`
	Mesage string `json:"message"`
	Error  string `json:"error"`
}

func (s *HTTPServer) bindEndpoints(router *gin.Engine) {
	for _, endpoint := range s.Config.Endpoints {
		if endpoint.IsGRPC {
			continue
		}
		httpMethod := endpoint.HTTPMethod
		if httpMethod == "" {
			httpMethod = "GET"
		}
		router.Handle(httpMethod, endpoint.Path, func(c *gin.Context) {
			var data []byte
			var err error
			if c.Request.Body != nil {
				data, err = io.ReadAll(c.Request.Body)
				if err != nil {
					c.JSON(500, &ErrorMessage{
						Status: 500,
						Mesage: "Failed to read request input",
						Error:  err.Error(),
					})
					return
				}
			}

			headers := map[string]string{}
			for key, value := range c.Request.Header {
				headers[fmt.Sprintf("KBRG-HTTP-HEADER-%s", key)] = value[0]
			}

			message := &connector.Message{
				ID:      connector.NewMessageID("KBRG-HTTP-", 16),
				Type:    endpoint.DataType,
				Port:    "http",
				Payload: data,
				Headers: headers,
			}

			opts := &connector.SendOptions{
				Topic:          endpoint.Kafka.Topic,
				Partition:      endpoint.Kafka.Partition,
				ReplyTopic:     endpoint.Kafka.ReplyTopic,
				ReplyPartition: endpoint.Kafka.ReplyPartition,
				Passthrough:    endpoint.Passthrough,
			}

			s.kafkaConnector.RequestReply(message, opts, func(reply []byte, headers connector.MessageHeaders, err error) {
				respStatusCode := 200
				respContentType := "application/octet-stream"
				var e error

				if headers != nil {
					respStatusCodeStr := headers.GetString("KBRG-HTTP-RESPONSE-CODE")
					if respStatusCodeStr != "" {
						respStatusCode, e = strconv.Atoi(respStatusCodeStr)
						if e != nil {
							log.Error().Str("error", e.Error()).Msg("Failed to read HTTP Response Code")
						}
					}
					respContentTypeStr := headers.GetString("KBRG-HTTP-HEADER-Content-Type")
					if respContentTypeStr != "" {
						respContentType = respContentTypeStr
					}

					for key, headerGenericValue := range headers {
						if strings.HasPrefix(key, "KBRG-HTTP-HEADER-") {
							headerName := strings.TrimPrefix(key, "KBRG-HTTP-HEADER-")
							if headerValueStr, ok := headerGenericValue.(string); ok {
								c.Header(headerName, headerValueStr)
								continue
							}
							if headerValueBytes, ok := headerGenericValue.([]byte); ok {
								c.Header(headerName, string(headerValueBytes))
								continue
							}
						}
					}

				}

				c.Data(respStatusCode, respContentType, reply)

			})
		})
		log.Info().Str("path", endpoint.Path).Msgf("Endpoint: %s", endpoint.Path)
	}
}

func (s *HTTPServer) Run() error {

	router := gin.Default()

	address := fmt.Sprintf("%s:%d", s.Config.Server.HTTPConfig.Host, s.Config.Server.HTTPConfig.Port)

	s.bindEndpoints(router)

	log.Info().Str("address", address).Msgf("HTTP Server running on: %s", address)
	return router.Run(address)
}

func NewHTTPServer(config *kbridge.Config, conn connector.Connector) *HTTPServer {

	return &HTTPServer{
		Config:         config,
		kafkaConnector: conn,
	}
}
