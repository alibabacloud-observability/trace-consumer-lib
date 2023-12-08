package com.aliyun.arms.trace.convert;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.arms.trace.model.Span;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author carpela <luhao.wh@alibaba-inc.com>
 * @date 2023/12/8
 */
public class OtelTraceConverter {

    private static OtlpHttpSpanExporter exporter = null;

    public static void convertSpanAndHandle(Map<String, String> items) {
        try {
            Span span = new Span();
            span.setTraceId(items.get("traceId"));
            span.setDuration(Long.parseLong(items.get("duration")));
            span.setSpanId(items.get("spanId"));
            span.setPSpanId(items.get("parentSpanId"));
            span.setStartTime(Long.parseLong(items.get("startTime")));
            span.setSpanName(items.get("spanName"));
            span.setServiceName(items.get("serviceName"));
            span.setPid(items.get("pid")); // ARMS应用ID
            span.setIp((items.get("ip")));
            span.setKind(Integer.parseInt(items.get("kind")));
            span.setHostname(items.get("hostname"));
            span.setStatus(Integer.parseInt(items.get("statusCode")));
            span.setStatusMessage(items.get("statusMessage"));
            span.setTraceState(items.get("traceState"));
            Map<String, String> attrs = JSONObject.parseObject(items.get("attributes"), Map.class);
            if (attrs != null) {
                span.getAttributesMap().putAll(attrs);
            }
            Map<String, String> resources = JSONObject.parseObject(items.get("resources"), Map.class);
            if (resources != null) {
                span.getResourcesMap().putAll(resources);
            }
            span.getResourcesMap().put("service.name", items.get("serviceName"));
            if (items.get("ip") != null) {
                span.getResourcesMap().put("ipv4", items.get("ip"));
            }
            List<Span.Event> events = JSONObject.parseArray(items.get("events"), Span.Event.class);
            if (events != null) {
                span.getEventList().addAll(events);
            }
            List<Span.Link> links = JSONObject.parseArray(items.get("links"), Span.Link.class);
            if (links != null) {
                span.getLinkList().addAll(links);
            }

            handle(span);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public static void setExporter(OtlpHttpSpanExporter exporter) {
        OtelTraceConverter.exporter = exporter;
    }

    public static void handle(Span span) {
        if (exporter != null) {
            exporter.export(Collections.singleton(span));
        }
    }

}
