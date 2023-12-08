package com.aliyun.arms.trace.source;

/**
 * @author carpela <luhao.wh@alibaba-inc.com>
 * @date 2023/12/8
 */

import com.aliyun.arms.trace.convert.OtelTraceConverter;
import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogContent;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SLSConsumer implements ILogHubProcessor {
    private int shardId;
    // 记录上次持久化Checkpoint的时间。
    private long mLastCheckTime = 0;

    public void initialize(int shardId) {
        this.shardId = shardId;
    }

    // 消费数据的主逻辑，消费时的所有异常都需要处理，不能直接抛出。
    public String process(List<LogGroupData> logGroups,
                          ILogHubCheckPointTracker checkPointTracker) {
        // 打印已获取的数据。
        for (LogGroupData logGroup : logGroups) {
            FastLogGroup flg = logGroup.GetFastLogGroup();
            for (int lIdx = 0; lIdx < flg.getLogsCount(); ++lIdx) {
                FastLog log = flg.getLogs(lIdx);
                Map<String, String> logItems = new HashMap<>(log.getContentsCount());

//                System.out.println("--------\nLog: " + lIdx + ", time: " + log.getTime() + ", GetContentCount: " + log.getContentsCount());

                for (int cIdx = 0; cIdx < log.getContentsCount(); ++cIdx) {
                    FastLogContent content = log.getContents(cIdx);
                    logItems.put(content.getKey(), content.getValue());
//                    System.out.println(content.getKey() + "\t:\t" + content.getValue());
                }
                OtelTraceConverter.convertSpanAndHandle(logItems);
            }
        }
        long curTime = System.currentTimeMillis();
        // 每隔30秒，写一次Checkpoint到服务端。如果30秒内发生Worker异常终止，新启动的Worker会从上一个Checkpoint获取消费数据，可能存在少量的重复数据。
        if (curTime - mLastCheckTime > 30 * 1000) {
            try {
                //参数为true表示立即将Checkpoint更新到服务端；false表示将Checkpoint缓存在本地。默认间隔60秒会将Checkpoint更新到服务端。
                checkPointTracker.saveCheckPoint(true);
            } catch (LogHubCheckPointException e) {
                e.printStackTrace();
            }
            mLastCheckTime = curTime;
        }
        return null;
    }

    // 当Worker退出时，会调用该函数，您可以在此处执行清理工作。
    public void shutdown(ILogHubCheckPointTracker checkPointTracker) {
        // 将Checkpoint立即保存到服务端。
        try {
            checkPointTracker.saveCheckPoint(true);
        } catch (LogHubCheckPointException e) {
            e.printStackTrace();
        }
    }
}