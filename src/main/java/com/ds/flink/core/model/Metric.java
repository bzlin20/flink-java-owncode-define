package com.ds.flink.core.model;

import java.util.Map;

public class Metric {
    public String name;
    public long timestamp;
    public Map<String, Object> fields;
    public Map<String, String> tags;

    public  Metric(){}

    public Metric(String name, long timestamp, Map<String, Object> fields, Map<String, String> tags) {
        this.name = name;
        this.timestamp = timestamp;
        this.fields = fields;
        this.tags = tags;
    }

    // CONTROL + ENTER
    @Override
    public String toString() {
        return "Metric{" +
                "name='" + name + '\'' +
                ", timestamp=" + timestamp +
                ", fields=" + fields +
                ", tags=" + tags +
                '}';
    }

    public String getName() {
        return name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setFields(Map<String, Object> fields) {
        this.fields = fields;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }
}
