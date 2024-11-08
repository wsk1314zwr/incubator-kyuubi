package org.apache.kyuubi.plugin.spark.authz.security;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * datark 存储的的用户所有数据权限信息
 */
public class DatarkUserAuthedPermissionInfo {

    /**
     * 用户是否存在(即是否登陆过datark),true:存在,fasle:不存在
     */
    private boolean exist;

    /**
     * 是否是管理员,true:是,fasle：否
     */
    private boolean isAdmin;

    /**
     * 查询权限的时间，也就是权限组装的时间
     */
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date queryTime;

    public Date getQueryTime() {
        return queryTime;
    }

    public void setQueryTime(Date queryTime) {
        this.queryTime = queryTime;
    }

    /**
     * 拥有库的权限信息,当拥有库下所有表ALL/SELECT权限时，将不会显示具体表、字段的权限,key:库名
     */
    private HashMap<String, DataBasePermissionInfo> dataBasePermissionInfos;

    /**
     * 拥有表的权限信息,当表有SELECT权限时，将不会显示具体字段的权限,key：库名
     */
    private HashMap<String, List<TablePermissionInfo>> tablePermissionInfos;

    /**
     * 拥有字段的权限信息,当表有SELECT权限时，将不会显示具体字段的权限,key：库名.表名
     */
    private HashMap<String, List<FieldPermissionInfo>> fieldPermissionInfos;

    /**
     * 人员所在空间的配置的表的行级过滤信息列表，admin为空,key：库名.表, value:过滤表达式
     */
    private HashMap<String, String> rowFilterConfigInfo;

    public boolean isExist() {
        return exist;
    }

    public void setExist(boolean exist) {
        this.exist = exist;
    }

    public boolean isAdmin() {
        return isAdmin;
    }

    public void setAdmin(boolean admin) {
        isAdmin = admin;
    }

    public HashMap<String, DataBasePermissionInfo> getDataBasePermissionInfos() {
        return dataBasePermissionInfos;
    }

    public void setDataBasePermissionInfos(HashMap<String, DataBasePermissionInfo> dataBasePermissionInfos) {
        this.dataBasePermissionInfos = dataBasePermissionInfos;
    }

    public HashMap<String, List<TablePermissionInfo>> getTablePermissionInfos() {
        return tablePermissionInfos;
    }

    public void setTablePermissionInfos(HashMap<String, List<TablePermissionInfo>> tablePermissionInfos) {
        this.tablePermissionInfos = tablePermissionInfos;
    }

    public HashMap<String, List<FieldPermissionInfo>> getFieldPermissionInfos() {
        return fieldPermissionInfos;
    }

    public void setFieldPermissionInfos(HashMap<String, List<FieldPermissionInfo>> fieldPermissionInfos) {
        this.fieldPermissionInfos = fieldPermissionInfos;
    }

    public HashMap<String, String> getRowFilterConfigInfo() {
        return rowFilterConfigInfo;
    }

    public void setRowFilterConfigInfo(HashMap<String, String> rowFilterConfigInfo) {
        this.rowFilterConfigInfo = rowFilterConfigInfo;
    }
}
