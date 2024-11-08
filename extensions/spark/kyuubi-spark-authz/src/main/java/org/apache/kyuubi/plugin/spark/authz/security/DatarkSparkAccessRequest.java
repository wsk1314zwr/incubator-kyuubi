package org.apache.kyuubi.plugin.spark.authz.security;

import org.apache.kyuubi.plugin.spark.authz.ranger.AccessResource;

import java.util.Date;

/**
 * spark 权限校验请求内容
 */
public class DatarkSparkAccessRequest {
    private AccessResource resource;
    private String user;
    private Date accessTime;
    private String action;
    private String accessType;
    private String datarkUrl;
    private String appCode;

    /**
     * 权限缓存过期时间，单位分钟
     */
    private Integer expireTime;

    private String auditEnable;

    private String datarkQueryType;

    private String datarkTaskId;

    private String projectCode;

    public String getAuditEnable() {
        return auditEnable;
    }

    public void setAuditEnable(String auditEnable) {
        this.auditEnable = auditEnable;
    }

    public String getDatarkQueryType() {
        return datarkQueryType;
    }

    public void setDatarkQueryType(String datarkQueryType) {
        this.datarkQueryType = datarkQueryType;
    }

    public String getDatarkTaskId() {
        return datarkTaskId;
    }

    public void setDatarkTaskId(String datarkTaskId) {
        this.datarkTaskId = datarkTaskId;
    }

    public Integer getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(Integer expireTime) {
        this.expireTime = expireTime;
    }

    public DatarkSparkAccessRequest() {
    }

    public DatarkSparkAccessRequest(AccessResource resource, String user, String action, String accessType, String datarkUrl,
                                    String appcode, Integer expireTime, String auditEnable, String datarkQueryType, String datarkTaskId,
                                    String projectCode) {
        this.resource = resource;
        this.user = user;
        this.accessTime = new Date();
        this.action = action;
        this.accessType = accessType;
        this.datarkUrl = datarkUrl;
        this.appCode = appcode;
        this.expireTime = expireTime;
        this.auditEnable = auditEnable;
        this.datarkQueryType = datarkQueryType;
        this.datarkTaskId = datarkTaskId;
        this.projectCode = projectCode;
    }

    public String getAppCode() {
        return appCode;
    }

    public void setAppCode(String appCode) {
        this.appCode = appCode;
    }

    public String getDatarkUrl() {
        return datarkUrl;
    }

    public void setDatarkUrl(String datarkUrl) {
        this.datarkUrl = datarkUrl;
    }

    public AccessResource getResource() {
        return resource;
    }

    public void setResource(AccessResource resource) {
        this.resource = resource;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Date getAccessTime() {
        return accessTime;
    }

    public void setAccessTime(Date accessTime) {
        this.accessTime = accessTime;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getAccessType() {
        return accessType;
    }

    public void setAccessType(String accessType) {
        this.accessType = accessType;
    }

    public String getProjectCode() {
        return projectCode;
    }

    public void setProjectCode(String projectCode) {
        this.projectCode = projectCode;
    }

    public DatarkSparkAccessRequest copy() {
        DatarkSparkAccessRequest datarkSparkAccessRequest = new DatarkSparkAccessRequest();
        datarkSparkAccessRequest.setResource(getResource());
        datarkSparkAccessRequest.setUser(getUser());
        datarkSparkAccessRequest.setAccessTime(getAccessTime());
        datarkSparkAccessRequest.setAction(getAction());
        datarkSparkAccessRequest.setAccessType(getAccessType());
        datarkSparkAccessRequest.setDatarkUrl(getDatarkUrl());
        datarkSparkAccessRequest.setAppCode(getAppCode());
        datarkSparkAccessRequest.setExpireTime(getExpireTime());
        datarkSparkAccessRequest.setAuditEnable(getAuditEnable());
        datarkSparkAccessRequest.setDatarkQueryType(getDatarkQueryType());
        datarkSparkAccessRequest.setDatarkTaskId(getDatarkTaskId());
        datarkSparkAccessRequest.setProjectCode(getProjectCode());
        return datarkSparkAccessRequest;
    }
}
