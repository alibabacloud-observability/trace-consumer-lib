消费`可观测OpenTelemetry版`存储数据的 SLS logstore，并转换成标准的 Otlp 数据模型
通过 otlp-http-exporter 上报到另外的 endpoint

* 修改 TraceConsumer 里面的常量，按照实际情况调整
* 启动后即可上报