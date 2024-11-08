package org.apache.kyuubi.plugin.spark.authz.security;

/**
 * datark 存储的的用户库的权限信息
 */
public class DataBasePermissionInfo {

    /**
     * 数据库名称
     */
    private String dbName;

    /**
     * 库只有其下所有表的所有权限(ALL)或者使用权限(USE)或者所有表的查询权限(SELECT)
     */
    private String permissionType;

    public String getDbName() {

        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getPermissionType() {
        return permissionType;
    }

    public void setPermissionType(String permissionType) {
        this.permissionType = permissionType;
    }
}
