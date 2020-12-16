package com.aliyun.odps.spark.examples.models;

import java.io.Serializable;

public class ItemBase implements Serializable {
    private String id;
    private String url;
    private String version;
    private String crawer_time;
    private String content;
    private String pp;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getCrawer_time() {
        return crawer_time;
    }

    public void setCrawer_time(String crawer_time) {
        this.crawer_time = crawer_time;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getPp() {
        return pp;
    }

    public void setPp(String pp) {
        this.pp = pp;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}