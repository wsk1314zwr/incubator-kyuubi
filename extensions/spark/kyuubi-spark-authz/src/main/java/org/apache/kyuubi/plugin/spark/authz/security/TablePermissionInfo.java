package org.apache.kyuubi.plugin.spark.authz.security;

/**
 * datark 存储的的用户表的权限信息
 */
public class TablePermissionInfo {

    /**
     * 数据表名称
     */
    private String tableName;

    /**
     * 若只有表权限，则只有表查询SELECT权限(SELECT)或者使用权限(USE)
     */
    private String permissionType;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPermissionType() {
        return permissionType;
    }

    public void setPermissionType(String permissionType) {
        this.permissionType = permissionType;
    }
}
