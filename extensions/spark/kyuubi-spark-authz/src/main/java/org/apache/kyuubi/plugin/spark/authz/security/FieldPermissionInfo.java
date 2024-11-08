package org.apache.kyuubi.plugin.spark.authz.security;

/**
 * datark 存储的的用户表字段的权限信息
 */
public class FieldPermissionInfo {
    /**
     * 字段名称
     */
    private String columnName;

    /**
     * 若只有字段权限，则只有字段的查询SELECT权限(SELECT)
     */
    private String permissionType;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getPermissionType() {
        return permissionType;
    }

    public void setPermissionType(String permissionType) {
        this.permissionType = permissionType;
    }
}
