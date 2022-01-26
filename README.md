# kbridge
HTTP/gRPC bridge to Apache Kafka topics

# Features

* Configurable endpoints, either REST or otherwise. YAML configuration:
```yaml
server:
  host: 0.0.0.0
  port: 3000

kafka:
  kafkaUrl: "kafla.local.cluster:9094"

endpoints:
- url: "/products/:prodId"
  method: "GET"
  topic: "products"
  reply-topic: "products-response"
- url: "/products"
  method: "POST"
  topic: "products-get"
```

* Configuration by endpoint
 * URL path templates with Go Gin
 * Support for path variables
 * Support for query parameters

* gRPC support
 * custom protobuf schemas (?)


