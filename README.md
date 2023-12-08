消费`可观测OpenTelemetry版`存储数据的 SLS logstore，并转换成标准的 Otlp 数据模型
通过 otlp-http-exporter 上报到另外的 endpoint

1. 修改 TraceConsumer 里面的 AK、SK
2. 修改 TraceConsumer 里面的 endpoint
3. 启动后即可上报