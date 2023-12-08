package com.aliyun.arms.trace.source;

import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;

/**
 * @author carpela <luhao.wh@alibaba-inc.com>
 * @date 2023/12/8
 */
public class LogHubProcessorFactory implements ILogHubProcessorFactory {
    public ILogHubProcessor generatorProcessor() {
        // 生成一个消费实例。
        return new SLSConsumer();
    }
}