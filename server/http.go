package server

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/natemago/kbridge"
	"github.com/rs/zerolog/log"
)

type HTTPServer struct {
	Config *kbridge.Config
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

func NewHTTPServer(config *kbridge.Config) *HTTPServer {
	return &HTTPServer{
		Config: config,
	}
}
