package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/natemago/kbridge"
	"github.com/natemago/kbridge/connector"
	"github.com/rs/zerolog/log"
)

type HTTPServer struct {
	Config         *kbridge.Config
	kafkaConnector connector.Connector
	httpServer     *http.Server
	running        bool
	runMux         sync.Mutex
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

			variables := map[string]string{}
			for _, param := range c.Params {
				variables[param.Key] = param.Value
			}

			message := &connector.Message{
				ID:         connector.NewMessageID("KBRG-HTTP", 16),
				Type:       endpoint.DataType,
				Port:       "http",
				Path:       c.Request.URL.Path,
				Payload:    data,
				Headers:    headers,
				Variables:  variables,
				Parameters: c.Request.URL.Query(),
			}

			opts := &connector.SendOptions{
				Topic:          endpoint.Kafka.Topic,
				Partition:      endpoint.Kafka.Partition,
				ReplyTopic:     endpoint.Kafka.ReplyTopic,
				ReplyPartition: endpoint.Kafka.ReplyPartition,
				Passthrough:    endpoint.Passthrough,
			}

			replyDone := make(chan bool)
			s.kafkaConnector.RequestReply(message, opts, func(reply []byte, headers connector.MessageHeaders, err error) {
				defer func() {
					replyDone <- true
				}()

				if err != nil {
					log.Error().Err(err).Msgf("Reply failed: %s", err.Error())
					if connector.IsErrorOfType("timeout", err) {
						c.JSON(504, &ErrorMessage{
							Status: 504,
							Mesage: "timeout",
							Error:  err.Error(),
						})
						return
					}
					c.JSON(502, &ErrorMessage{
						Status: 502,
						Mesage: "transport error",
						Error:  err.Error(),
					})
					return
				}

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

			<-replyDone
		})

		log.Info().Str("path", endpoint.Path).Msgf("Endpoint: %s", endpoint.Path)
	}
}

func (s *HTTPServer) Run() error {
	s.runMux.Lock()
	if s.running {
		s.runMux.Unlock()
		return fmt.Errorf("http server already running")
	}
	s.running = true

	router := gin.Default()

	address := fmt.Sprintf("%s:%d", s.Config.Server.HTTPConfig.Host, s.Config.Server.HTTPConfig.Port)

	s.httpServer = &http.Server{
		Addr:    address,
		Handler: router,
	}

	s.bindEndpoints(router)

	log.Info().Str("address", address).Msgf("HTTP Server running on: %s", address)
	s.runMux.Unlock()
	return s.httpServer.ListenAndServe()
}

func (s *HTTPServer) Shutdown(timeout time.Duration) error {
	s.runMux.Lock()
	if !s.running {
		s.runMux.Unlock()
		return fmt.Errorf("not running")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err := s.httpServer.Shutdown(ctx)
	s.running = false
	s.runMux.Unlock()
	return err
}

func NewHTTPServer(config *kbridge.Config, conn connector.Connector) *HTTPServer {
	return &HTTPServer{
		Config:         config,
		kafkaConnector: conn,
	}
}
