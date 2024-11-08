package org.apache.kyuubi.plugin.spark.authz.security;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessResource;
import org.apache.kyuubi.plugin.spark.authz.util.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Enumeration;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * 权限校验
 */
public class DatarkSparkAuthentication {

    public static final Logger logger = LoggerFactory.getLogger(DatarkSparkAuthentication.class);

    public static final String DATARK_GET_USER_PRI_PATH = "/datark/api/privilege/queryUser";

    public static final String DATARK_PRI_AUDIT_PATH = "/datark/api/privilege/storeAuditLogs";

    public final static ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .setTimeZone(TimeZone.getDefault());

    public static final Cache<String, DatarkUserAuthedPermissionInfo> userPermissionInfos = Caffeine.newBuilder()
            .initialCapacity(10)
            .expireAfterWrite(24 * 60, TimeUnit.MINUTES)
            .maximumSize(2000)
            .build();

    public static ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("store-audit-pool-%d").setDaemon(true).build();
    public static ExecutorService executorService = new ThreadPoolExecutor(5, 5, 2, TimeUnit.SECONDS, new LinkedBlockingDeque<>(100000), threadFactory, new ThreadPoolExecutor.DiscardPolicy());

    public static boolean isAccessAllowed(DatarkSparkAccessRequest request, Boolean audit) {
        boolean accessAllowed = isAccessAllowed(request);
        if (audit && "true".equalsIgnoreCase(request.getAuditEnable())) {
            audit(request, accessAllowed);
        }
        return accessAllowed;
    }

    public static boolean isAccessAllowed(DatarkSparkAccessRequest request) {
        DatarkUserAuthedPermissionInfo permissionInfo = getUserPermissionInfo(request.getUser(), request.getAppCode(), request.getDatarkUrl(), request.getExpireTime(), request.getProjectCode());
        //无法获取用户信息，如datark api重启中、网络中断,权限直接判定通过
        if (Objects.isNull(permissionInfo)) {
            return true;
        }
        //若用户不存在，则无操作spark sql权限
        if (!permissionInfo.isExist()) {
            return false;
        }
        //若用户是admin，则拥有所有的权限
        if (permissionInfo.isAdmin()) {
            return true;
        }

        String accessType = request.getAccessType();
        AccessResource resource = request.getResource();
        String database = resource.getDatabase();
        String table = resource.getTable();
        String column = resource.getColumn();
        Enumeration.Value objectType = resource.getObjectType();
        if ("DATABASE".equalsIgnoreCase(objectType.toString())) {
            //校验数据库的权限
            return checkDatabasesPermission(permissionInfo, database, accessType);
        } else if ("TABLE".equalsIgnoreCase(objectType.toString())) {
            //校验数据表的权限
            return checkTablePermission(permissionInfo, database, table, accessType);
        } else if ("VIEW".equalsIgnoreCase(objectType.toString())) {
            //校验VIEW的权限
            return checkViewPermission(permissionInfo, database, table, accessType);
        } else if ("COLUMN".equalsIgnoreCase(objectType.toString())) {
            // 校验列的权限
            return checkColumnPermission(permissionInfo, database, table, column, accessType);
        } else {
            //其他资源，当前不校验权限，直接通过
            return true;
        }
    }

    private static boolean checkColumnPermission(DatarkUserAuthedPermissionInfo permissionInfo, String database, String table, String column, String accessType) {
        // 数据库为空或者表或者字段为空或者访问权限无，则权限校验通过
        if (StringUtils.isBlank(database) || "*".equalsIgnoreCase(database) || StringUtils.isBlank(table) || StringUtils.isBlank(column) || "NONE".equalsIgnoreCase(accessType) || StringUtils.isEmpty(accessType)) {
            return true;
        }
        DataBasePermissionInfo dataBasePermissionInfo = permissionInfo.getDataBasePermissionInfos().get(database.toLowerCase());
        //拥有库的ALL权限，则列的所有操作都可以通过
        if (Objects.nonNull(dataBasePermissionInfo) && "ALL".equalsIgnoreCase(dataBasePermissionInfo.getPermissionType())) {
            return true;
        }

        boolean isAllowed = false;
        //列级权限只校验SELECT权限，其它的访问权限都是被禁止,SELECT权限对应的操作Commond有:QUERY
        if ("SELECT".equals(accessType.toUpperCase())) {//拥有库下所有表的的SELECT权限,则拥有列的USE、SELECT权限
            if (Objects.nonNull(dataBasePermissionInfo) && "SELECT".equalsIgnoreCase(dataBasePermissionInfo.getPermissionType())) {
                isAllowed = true;
                return isAllowed;
            }
            List<TablePermissionInfo> tablePermissionInfos = permissionInfo.getTablePermissionInfos().get(database.toLowerCase());
            TablePermissionInfo tablePermissionInfo = null;
            if (CollectionUtils.isNotEmpty(tablePermissionInfos)) {
                Map<String, TablePermissionInfo> tables = tablePermissionInfos.stream().collect(Collectors.toMap(TablePermissionInfo::getTableName, c -> c, (a, b) -> a));
                tablePermissionInfo = tables.get(table.toLowerCase());
            }
            if (Objects.nonNull(tablePermissionInfo) && "SELECT".equalsIgnoreCase(tablePermissionInfo.getPermissionType())) {
                //拥有列对应的表的SELECT权限，则拥有该列USE、SELECT权限
                isAllowed = true;
            } else {
                List<FieldPermissionInfo> fieldPermissionInfos = permissionInfo.getFieldPermissionInfos().get(database + "." + table);
                if (CollectionUtils.isNotEmpty(fieldPermissionInfos)) {
                    isAllowed = fieldPermissionInfos.stream().map(FieldPermissionInfo::getColumnName).anyMatch(column::equalsIgnoreCase);
                }
            }
        }

        return isAllowed;
    }

    /**
     * 检查操作VIEW的权限
     */
    private static boolean checkViewPermission(DatarkUserAuthedPermissionInfo permissionInfo, String database, String view, String accessType) {
        // 数据库为空或者视图为空或者访问权限无，则权限校验通过
        if (StringUtils.isBlank(database) || "*".equalsIgnoreCase(database) || StringUtils.isBlank(view) || "NONE".equalsIgnoreCase(accessType) || StringUtils.isEmpty(accessType)) {
            return true;
        }
        DataBasePermissionInfo dataBasePermissionInfo = permissionInfo.getDataBasePermissionInfos().get(database.toLowerCase());
        /*
         * 拥有库的ALL权限，则表的所有操作都可以通过
         * VIEW资源权限对应的操作Commond只有：ALTERVIEW_RENAME ｜CREATEVIEW ｜DROPVIEW
         * QUERY VIEW走的是底层TABLE的SELET权限
         */
        return Objects.nonNull(dataBasePermissionInfo) && "ALL".equalsIgnoreCase(dataBasePermissionInfo.getPermissionType());
    }

    /**
     * 检查操作表的权限
     */
    private static boolean checkTablePermission(DatarkUserAuthedPermissionInfo permissionInfo, String database, String table, String accessType) {
        // 数据库为空或者表为空或者访问权限无，则权限校验通过
        if (StringUtils.isBlank(database) || "*".equalsIgnoreCase(database) || StringUtils.isBlank(table) || "NONE".equalsIgnoreCase(accessType) || StringUtils.isEmpty(accessType)) {
            return true;
        }
        DataBasePermissionInfo dataBasePermissionInfo = permissionInfo.getDataBasePermissionInfos().get(database.toLowerCase());
        //拥有库的ALL权限，则表的所有操作都可以通过
        if (Objects.nonNull(dataBasePermissionInfo) && "ALL".equalsIgnoreCase(dataBasePermissionInfo.getPermissionType())) {
            return true;
        }
        List<TablePermissionInfo> tablePermissionInfos = permissionInfo.getTablePermissionInfos().get(database.toLowerCase());
        TablePermissionInfo tablePermissionInfo = null;
        if (CollectionUtils.isNotEmpty(tablePermissionInfos)) {
            Map<String, TablePermissionInfo> tables = tablePermissionInfos.stream().collect(Collectors.toMap(TablePermissionInfo::getTableName, c -> c, (a, b) -> a));
            tablePermissionInfo = tables.get(table.toLowerCase());
        }
        /*
         * 表级的权限只校验SELECT、USE权限，其他所有权限都不通过，对应的Commond命令如下：
         * select:QUERY | SHOW_CREATETABLE | SHOWPARTITIONS | SHOW_TBLPROPERTIES
         * use:DESCTABLE,SHOWTABLES
         */
        boolean isAllowed = false;
        switch (accessType.toUpperCase()) {
            case "SELECT":
                //拥有库下所有表的的SELECT权限
                if (Objects.nonNull(dataBasePermissionInfo) && "SELECT".equalsIgnoreCase(dataBasePermissionInfo.getPermissionType())) {
                    isAllowed = true;
                } else if (Objects.nonNull(tablePermissionInfo) && "SELECT".equalsIgnoreCase(tablePermissionInfo.getPermissionType())) {
                    isAllowed = true;
                }
                break;
            case "USE":
                //拥有库下所有表的的SELECT权限，则拥有当前表的USE权限
                if (Objects.nonNull(dataBasePermissionInfo) && "SELECT".equalsIgnoreCase(dataBasePermissionInfo.getPermissionType())) {
                    isAllowed = true;
                } else if (Objects.nonNull(tablePermissionInfo)) {
                    isAllowed = true;
                }
                break;
            default:
                isAllowed = false;
        }
        return isAllowed;
    }

    /**
     * 检查操作数据库的权限
     */
    private static boolean checkDatabasesPermission(DatarkUserAuthedPermissionInfo permissionInfo, String database, String accessType) {
        // 数据库为空，或者访问权限无，则权限校验通过
        if (StringUtils.isBlank(database) || "*".equalsIgnoreCase(database) || "NONE".equalsIgnoreCase(accessType) || StringUtils.isEmpty(accessType)) {
            return true;
        }
        boolean isAllowed;
        //普通用户只能有database的use权限，对应的Commond操作是：SHOWDATABASES | SWITCHDATABASE | DESCDATABASE,禁止执行：ALTERDATABASE、CREATEDATABASE、DROPDATABASE等命令
        if ("USE".equals(accessType.toUpperCase())) {
            isAllowed = permissionInfo.getDataBasePermissionInfos().keySet().stream().anyMatch(database::equalsIgnoreCase);
        } else {
            isAllowed = false;
        }
        return isAllowed;
    }

    private  static DatarkUserAuthedPermissionInfo getUserPermissionInfo(String userName, String appCode, String datarkUrl, Integer expireTime, String projectCode) {
        DatarkUserAuthedPermissionInfo permissionInfo = userPermissionInfos.getIfPresent(userName);
        // 判断缓存是否过期，若过期则主动抛弃
        if (Objects.nonNull(permissionInfo)) {
            Date expireDate = DateUtils.addMinutes(new Date(), -expireTime);
            if (expireDate.compareTo(permissionInfo.getQueryTime()) > 0) {
                userPermissionInfos.invalidate(userName);
                permissionInfo = null;
            }
        }
        if (Objects.isNull(permissionInfo)) {
            synchronized (DatarkSparkAuthentication.class) {
                permissionInfo = userPermissionInfos.getIfPresent(userName);
                if (Objects.isNull(permissionInfo)) {
                    permissionInfo = getDatarkPermissionInfo(userName, appCode, datarkUrl, projectCode);
                    if (Objects.nonNull(permissionInfo)) {
                        userPermissionInfos.put(userName, permissionInfo);
                    }
                }
            }
        }
        return permissionInfo;
    }

    /**
     * 调用http接口查询用户权限信息
     *
     * @param appCode   appCode
     * @param userName  用户名称
     * @param datarkUrl datark地址
     * @return 权限信息
     */
    private static DatarkUserAuthedPermissionInfo getDatarkPermissionInfo(String userName, String appCode, String datarkUrl, String projectCode) {
        long start = System.currentTimeMillis();
        StringBuilder stringBuffer = new StringBuilder(datarkUrl);
        stringBuffer.append(DATARK_GET_USER_PRI_PATH).append("?userName=").append(userName);
        if (!StringUtils.isBlank(projectCode)) {
            stringBuffer.append("&projectCode=").append(projectCode);
        }
        HashMap<String, String> heads = new HashMap<>();
        heads.put("appCode", appCode);
        DatarkUserAuthedPermissionInfo userAuthedPermissionInfo = null;
        int responseSize = 0;
        try {
            String responseContent = HttpUtils.get(stringBuffer.toString(), heads);
            JsonNode jsonObject;
            jsonObject = OBJECT_MAPPER.readTree(responseContent);
            responseSize = responseContent.getBytes().length;
            logger.info("calling datark to get user permission info,response content: \n {}", responseContent);
            String status = jsonObject.get("head").get("status").textValue();
            if (!"Y".equalsIgnoreCase(status)) {
                String errMsg = String.format("calling datark to get user permission info failed, response msg [%s]", jsonObject.get("head").get("msg").textValue());
                throw new RuntimeException(errMsg);
            }
            userAuthedPermissionInfo = OBJECT_MAPPER.readValue(jsonObject.get("body").toString(), DatarkUserAuthedPermissionInfo.class);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        logger.info("calling datark to get user permission info cost {} milliseconds, response size {}b", System.currentTimeMillis() - start, responseSize);
        return userAuthedPermissionInfo;
    }

    /**
     * 记录权限校验日志
     *
     * @param request DatarkSparkAccessRequest
     * @param accessAllowed accessAllowed
     */
    private static void audit(DatarkSparkAccessRequest request, boolean accessAllowed) {
        executorService.execute(() -> {
            try {
                String msg = String.format("user [%s] access [%s] resource [%s] permission accessAllowed [%s], datarkQueryType[%s], datarkTaskId[%s]",
                        request.getUser(), request.getAccessType(), request.getResource().getAsString(), accessAllowed, request.getDatarkQueryType(), request.getDatarkTaskId());
                HashMap<String, String> heads = new HashMap<>();
                heads.put("appCode", request.getAppCode());
                HttpUtils.post(msg, request.getDatarkUrl() + DATARK_PRI_AUDIT_PATH, heads);
            } catch (Exception e) {
                logger.warn(e.getMessage());
            }
        });
    }

    /**
     * 获取表的行级过滤表达式
     *
     * @return 过滤表达式
     */
    public static String getTableRowFilterExp(String userName, String appCode, String datarkUrl, Integer expireTime,
                                              String projectCode, String dbTableName) {
        if (StringUtils.isBlank(dbTableName)) {
            return null;
        }
        DatarkUserAuthedPermissionInfo userPermissionInfo = getUserPermissionInfo(userName, appCode, datarkUrl, expireTime, projectCode);
        if (Objects.isNull(userPermissionInfo)) {
            return null;
        }
        if (!Objects.isNull(userPermissionInfo.getRowFilterConfigInfo())) {
            return userPermissionInfo.getRowFilterConfigInfo().get(dbTableName.toLowerCase());
        }
        return null;
    }

    /**
     * 是否有行级别过滤表达式
     *
     * @return boolean
     */
    public static boolean haveRowFilterConfigs(String userName, String appCode, String datarkUrl, Integer expireTime, String projectCode) {
        DatarkUserAuthedPermissionInfo userPermissionInfo = getUserPermissionInfo(userName, appCode, datarkUrl, expireTime, projectCode);
        if (Objects.isNull(userPermissionInfo)) {
            return false;
        }
        return !Objects.isNull(userPermissionInfo.getRowFilterConfigInfo()) && userPermissionInfo.getRowFilterConfigInfo().size() > 0;
    }
}
