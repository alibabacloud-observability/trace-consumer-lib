package com.aliyun.arms.trace;

import com.aliyun.arms.trace.convert.OtelTraceConverter;
import com.aliyun.arms.trace.source.LogHubProcessorFactory;
import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;

/**
 * @author carpela <luhao.wh@alibaba-inc.com>
 * @date 2023/12/8
 */
public class TraceConsumer {
    // 日志服务的服务接入点，请您根据实际情况填写。
    private static String Endpoint = "cn-hangzhou.log.aliyuncs.com";
    // 日志服务项目名称，请您根据实际情况填写。请从已创建项目中获取项目名称。
    private static String Project = "proj-xtrace-1d3031ecf7de5d16eabd5e4716a42cbc-cn-hangzhou";
    // 日志库名称，请您根据实际情况填写。请从已创建日志库中获取日志库名称。
    private static String Logstore = "logstore-tracing";
    // 消费组名称，请您根据实际情况填写。您无需提前创建，该程序运行时会自动创建该消费组。
    private static String ConsumerGroup = "consumerGroupX";
    // 拥有SLS Project 权限的子账号 AK、SK，注意生产中建议不要写死在代码中
    private static String AccessKeyId= "xxxxxx";
    private static String AccessKeySecret = "xxxxx";
    // OTLP HTTP 上报点
    private static String OtlpHttpEndpoint = "http://localhost:8080/adapt_xxx@xxx_aaa@xxx/api/otlp/traces";

    private static OtlpHttpSpanExporter exporter = OtlpHttpSpanExporter
            .builder()
            // HTTP 协议上报的 endpoint
            .setEndpoint(OtlpHttpEndpoint)
            .build();

    public static void main(String[] args) throws LogHubClientWorkerException, InterruptedException {
        // consumer_1是消费者名称，同一个消费组下面的消费者名称必须不同。不同消费者在多台机器上启动多个进程，均衡消费一个Logstore时，消费者名称可以使用机器IP地址来区分。
        // maxFetchLogGroupSize用于设置每次从服务端获取的LogGroup最大数目，使用默认值即可。您可以使用config.setMaxFetchLogGroupSize(100);调整，取值范围为(0,1000]。
        LogHubConfig config = new LogHubConfig(ConsumerGroup, "consumer_1",
                Endpoint, Project, Logstore, AccessKeyId, AccessKeySecret,
                LogHubConfig.ConsumePosition.BEGIN_CURSOR,1000);
        ClientWorker worker = new ClientWorker(new LogHubProcessorFactory(), config);

        OtelTraceConverter.setExporter(exporter);

        Thread thread = new Thread(worker);
        // Thread运行之后，ClientWorker会自动运行，ClientWorker扩展了Runnable接口。
        thread.start();
        Thread.sleep(60 * 60 * 1000);
        // 调用Worker的Shutdown函数，退出消费实例，关联的线程也会自动停止。
        worker.shutdown();
        // ClientWorker运行过程中会生成多个异步的任务。Shutdown完成后，请等待还在执行的任务安全退出。建议设置sleep为30秒。
        Thread.sleep(30 * 1000);
    }

}
