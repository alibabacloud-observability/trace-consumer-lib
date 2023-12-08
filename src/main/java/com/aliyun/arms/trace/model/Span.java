package com.aliyun.arms.trace.model;

/**
 * @author carpela <luhao.wh@alibaba-inc.com>
 * @date 2023/12/8
 */

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.resources.ResourceBuilder;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.StatusData;
import lombok.Data;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Setter
public class Span implements SpanData {
    private String traceId;
    private String pid;
    private String serviceName;
    private String spanName;
    private Long startTime;
    private Long duration;
    private String spanId;
    private String pSpanId;
    private String hostname;
    private String ip;
    private Integer kind;
    private Integer status;
    private String statusMessage;
    private String traceState;

    private List<Link> links = new ArrayList<>();

    private Map<String, String> attributes = new HashMap<>();

    private Map<String, String> resources = new HashMap<>();

    private List<Event> events = new ArrayList<>();

    @Override
    public String getName() {
        return spanName;
    }

    @Override
    public SpanKind getKind() {
        if (kind == 0) {
            return SpanKind.INTERNAL;
        } else if (kind == 1) {
            return SpanKind.SERVER;
        } else if (kind == 2) {
            return SpanKind.CLIENT;
        } else if (kind == 3) {
            return SpanKind.PRODUCER;
        } else if (kind == 4) {
            return SpanKind.CONSUMER;
        }
        return SpanKind.INTERNAL;
    }

    @Override
    public SpanContext getSpanContext() {
        return SpanContext.create(traceId,
                spanId,
                TraceFlags.fromHex("01", 0),
                TraceState.builder().build());
    }

    @Override
    public String getTraceId() {
        return traceId;
    }

    @Override
    public String getSpanId() {
        return spanId;
    }

    @Override
    public SpanContext getParentSpanContext() {
        return SpanContext.create(traceId, pSpanId, TraceFlags.fromHex("01", 0), TraceState.getDefault());
    }

    @Override
    public String getParentSpanId() {
        return SpanData.super.getParentSpanId();
    }

    @Override
    public StatusData getStatus() {
        if (status == 1) {
            return StatusData.ok();
        } else if (status == 2) {
            return StatusData.error();
        }
        return StatusData.unset();
    }

    @Override
    public long getStartEpochNanos() {
        return startTime;
    }

    @Override
    public Attributes getAttributes() {
        AttributesBuilder builder = Attributes.builder();
        for (String key : attributes.keySet()) {
            builder.put(key, attributes.get(key));
        }
        return builder.build();
    }

    @Override
    public List<EventData> getEvents() {
        List<EventData> eventData = new ArrayList<>();
        for (Event event : events) {
            AttributesBuilder builder = Attributes.builder();
            for (String key : event.getAttributes().keySet()) {
                builder.put(key, event.getAttributes().get(key));
            }
            EventData data = EventData.create(event.getTimestamp(), event.getName(), builder.build());
            eventData.add(data);
        }
        return eventData;
    }

    @Override
    public List<LinkData> getLinks() {
        List<LinkData> linkDataList = new ArrayList<>();
        for (Link link : links) {
            AttributesBuilder builder = Attributes.builder();
            for (String key : link.getAttributes().keySet()) {
                builder.put(key, link.getAttributes().get(key));
            }
            LinkData data = LinkData.create(SpanContext.create(link.getTraceId(), link.getSpanId(), TraceFlags.fromHex("01", 0), TraceState.getDefault()),  builder.build());
            linkDataList.add(data);
        }
        return linkDataList;
    }

    @Override
    public long getEndEpochNanos() {
        return startTime + duration;
    }

    @Override
    public boolean hasEnded() {
        return true;
    }

    @Override
    public int getTotalRecordedEvents() {
        return events.size();
    }

    @Override
    public int getTotalRecordedLinks() {
        return links.size();
    }

    @Override
    public int getTotalAttributeCount() {
        return attributes.size();
    }

    @Override
    public InstrumentationLibraryInfo getInstrumentationLibraryInfo() {
        return InstrumentationLibraryInfo.create("arms-trace-consumer-lib", "0.1");
    }

    @Override
    public Resource getResource() {
        ResourceBuilder builder = Resource.builder();
        for (String key : resources.keySet()) {
            builder.put(key, resources.get(key));
        }
        return builder.build();
    }

    public Map<String, String> getResourcesMap() {
        return resources;
    }

    public Map<String, String> getAttributesMap() {
        return attributes;
    }

    public List<Event> getEventList() {
        return events;
    }

    public List<Link> getLinkList() {
        return links;
    }

    @Data
    public static class Event {
        private long timestamp;
        private String name;
        private Map<String, String> attributes = new HashMap<>();
    }

    @Data
    public static class Link {
        private String traceId;
        private String spanId;
        private String traceState;
        private Map<String, String> attributes = new HashMap<>();
    }
}

