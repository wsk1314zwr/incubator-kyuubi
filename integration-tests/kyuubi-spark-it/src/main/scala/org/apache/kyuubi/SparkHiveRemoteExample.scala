package org.apache.kyuubi

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
//import org.apache.submarine.spark.security.Exception

/**
 * spark sql 权限冒烟测试
 *
 * 1）需要移除sentry的pom依赖，并放开spark-hive_2.12依赖的相干干hive包
 * 2）添加对应数仓的hive-site.xml(提供了是通过jdbc链接metasore数据库库或通过thrift连接metastore服务读写hive metastore)文件
 * 3）添加core-site.xml和hdfs-site.xml,这两个文件指明存放hive数据的hadoop的部署信息，不然无法读写hive数据
 * 4）添加spark.sql.hive想关的配置，指明hive的版本以及hive的相关jar包路径，不然会因为代码冲突报异常
 * 5) enableHiveSupport(),开启支持hive数仓
 * 5）若hive开启了kerberos，idea调测需要进行kerberos登录认证
 * 6) 若hive开启了kerberos，本机添加hive用户以及用户组信息，因为插入等操作，会校验权限，校验权限的第一步是获取本机的hive用户以及组的信息，无用户信息会报异常
 * 7) 添加yarn-site.xml，不然spark读取其自定义的表数据会报kerberos错误，但是加了会导致其它非需要kerberos任务(操作es)任务报错，需要移走，放在了resources/config下
 * 8）需将pom的hadooop依赖设置为hadoop3，不然会有些hadoop类找不到
 */

object SparkHiveRemoteExample extends Logging {


    // $example on:spark_hive$
    case class Record(key: Int, value: String)
    // $example off:spark_hive$

    def main(args: Array[String]) {
        HdfsKerberos.kerberos(new Configuration())
        val spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("Spark Hive Example")
                //添加hive 相关配置,不然无法操作hive
                .config("spark.sql.hive.metastore.version", "1.1.0")
                .config("spark.sql.hive.metastore.jars", "path")
                .config("spark.sql.hive.metastore.jars.path", "file:///opt/cloudera/parcels/CDH-5.16.1-1.cdh5.16.1.p0.3/lib/hive/lib/*")
                .enableHiveSupport()
                .config("spark.datark.security.authorization.query.env", "dev")
                //spark sql 权限校验相关配置
                .config("spark.sql.extensions", "org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension")
                //尝试排除规则，但是没成功，猜测SubmarineRowFilterExtension并不是AQE的规则列表的一部分，所以没有成功
//                .config("spark.sql.adaptive.enabled=","true")
//                .config("spark.sql.adaptive.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.SubmarineRowFilterExtension")
                .config("spark3.4.3.datark.security.authorization.enable", "true")
                .config("spark3.4.3.datark.security.authorization.failed.throwableException", "true")
//                .config("spark.datark.security.authorization.user", "wsk")
                .config("spark.datark.security.authorization.user", "ly6879")
//                .config("spark.datark.security.authorization.url", "http://127.0.0.1:8080")
                .config("spark.datark.security.authorization.url", "http://datark-manage-pc.datark-dev.devops.91lyd.com")
//                .config("spark.datark.security.authorization.url", "http://datark-manage-pc.servyou-release.devops.91lyd.com")
                // 调用open api的调用方的appcode
                .config("spark.datark.security.authorization.appcode", "spark_sql")
                .config("spark.datark.security.authorization.cache.expireAfterWrite", "20")
                .config("spark.datark.security.authorization.audit.enable", "true")
                //0:临时查询SQL 1:节点交互式查询SQL 2:测试实例SQL 3:周期实例SQL 4:补数据实例SQL
                .config("spark.datark.security.authorization.query.type", "1")
                .config("spark.datark.security.authorization.query.task.id", "1025")
                // 节点id所在空间的appcode，主要用于空间的行级别过滤
                .config("spark.datark.security.authorization.query.appcode", "mahq-datatest-002")
                .config("spark.datark.security.authorization.rowFilter.enable", "true")
                /**
                 * spark 集成hudi 并同步元数据到hive
                 * 1）开启如下两个config配置
                 * 2）一定是hive的jars路径<spark.sql.hive.metastore.jars.path>,增加hudi-hadoop-mr-bundle-0.10.1.jar、hudi-hive-sync-bundle-0.10.1.jar。直接放spark class path是无法读取的
                 * 3）spark cp下增加hudi-spark3.1.2-bundle_2.12-0.10.1.jar
                 */
//                .config("spark.sql.extensions", "org.apache.submarine.spark.security.api.RangerSparkAuthzExtension,org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
//                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                /**
                 * spark 血缘分析测试
                 * 1)开启如下2个配置
                 * 2)将血缘解析插件放入spark的cp
                 * 3)将血缘事件监听的插件放入spark的cp
                 */
//                .config("spark.sql.queryExecutionListeners", "org.apache.kyuubi.plugin.lineage2.SparkOperationLineageQueryExecutionListener")
//                .config("spark.extraListeners", "cn.com.servyou.data.lineage.spark.listener.SparkLineageEventListener")
                //开启task超时监控
                .config("spark.datark.task.run.timeout.monitor.enable", "true")
                .config("spark.datark.task.run.timeout.minute", "1")
                .config("spark.datark.task.run.timeout.alarm.url", "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=fa2a033b-3377-4f1b-b5ce-cad66f64b0cd")
                .getOrCreate()
        logInfo("这里是测试111")

        //测试1： show databases 是否展示有权限访问的库
        test1(spark)

        //测试2：测试use 切换库是否能切换到有权限访问的库
//        test2(spark)

        //测试3：测试 是否有权限create | drop databases;
//        test3(spark)

        //测试4：测试 show tables 是否只展示有权限访问的表
//        test4(spark)

        //测试5: 测试是否有权限crteate |drop table
//        test5(spark)

//        测试6: 测试是否有敏感字段权限
//        test6(spark)

//        测试7: 测试create table as 的权限
//        test7(spark)

        //测试8: 测试是否具有create drop view的权限
//        test8(spark)

        //测试9: SELECT * ViEW的权限
//        test9(spark)

        //测试10：insert into 权限
//        spark.sql("insert into hive_test.wsk_pt_m_lifecycle_test7 PARTITION(pt_d = '2021-01-01') select 1").collect().foreach(println(_))
//        spark.sql("select count(1) from hive_test.wsk_pt_m_lifecycle_test7 where pt_d = '2021-01-01' ").collect().foreach(println(_))

        //测试11：LOAD DATA 权限
//        sql("CREATE TABLE IF NOT EXISTS hive_test.wsk_test20220321 (key INT, value STRING) USING hive")
//        sql("LOAD DATA LOCAL INPATH '/Users/skwang/Documents/workspace/workspace4/project/open_project/submarine/submarine-security/spark-security/src/test/resources/data/files/kv1.txt' INTO TABLE hive_test.wsk_test20220321")


        //测试12：sqlDF
//        val sqlDF = sql("SELECT key, value FROM hive_test.wsk_test20220321  WHERE key < 10 ORDER BY key")
//        // The items in DataFrames are of type Row, which allows you to access each column by ordinal.
//        val stringsDS = sqlDF.map {
//            case Row(key: Int, value: String) => s"Key: $key, Value: $value"
//        }
//        stringsDS.show()

//        测试13：create temp view 权限
//        val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
//        recordsDF.createOrReplaceTempView("records")

        //测试14：join语法 权限
//        sql("SELECT * FROM records r JOIN hive_test.wsk_test20220321  s ON r.key = s.key").show()

        //测试14reset 重置session相关配置测试
//        sql("set datark.security.authorization.user=zhazhahhui")
//        sql("SELECT * FROM records r JOIN hive_test.wsk_test20220321  s ON r.key = s.key").show()

        //测试15：concat 函数解析测试
//        test15(spark)

        //测试17: 校验字段级权限进行， count(1),count(*),count(字段)
//        test17(spark)

        //测试18: insert overwrite语法权限校验，解决多error日志输出问题
//        test18(spark)

        //测试19: 临时视图创建以及查询权限校验
//        test19(spark)

        //测试20:临时视图操作es索引权限校验
//        test20(spark)

        //测试21:操作hudi权限校验
//        test21(spark)

        //测试22:操作1000列校验，压存储日志接口
//        test22(spark)

        //explain 一会有错一会儿无错报错处理
//        test23(spark)

//        //select '1''23' 未报错校验
//        test24(spark)

        //long overflow测试
//        test25(spark)

        //测试是否具有默认all权限库的 create｜ drop table的权限
//        test26(spark)
        //测试27: 测试DCL,查询所有hive角色
//        test27(spark)

        //测试28: 分区字段大写且带.,二次写入报错
//        test28(spark)

        //测试29: 测试spark 血缘插件
//        test29(spark)

        //测试30: 测试 join、where输入表级别血缘丢失问题
//        test30(spark)

//        //测试31: 测试 行级别过滤
//        test31(spark)

        //测试32: 测试 字段和表混乱
//        test32(spark)

        //测试33: orc snappy文件无法解压问题分析
//        test33(spark)

//        //测试34:  orc snappy文件无法解压问题分析,最终定位 表创建时未指定文件存储格式，使用默认的Text存储，最终生成的的是InsertIntoHiveTable Command，而使用orc存储的表生成的是InsertIntoHadoopFsRelationCommand
//        test34(spark)
//
//        //测试35: 只有select权限的表却能插入数据
//        test35(spark)

        //测试36:task超时监控
//        test36(spark)

        //测试37：两次drop无权限的表 第一次拒绝，第二次成功问题分析定位
//        test37(spark)

        //测试38：生产orc文件无法正确读取
        //test38(spark)


        spark.stop()

    }

    def test1(spark: SparkSession) = {
        //测试1： show databases 是否展示有权限访问的库
        spark.sql("show databases").collect().foreach(println(_))
    }


    def test2(spark: SparkSession) = {
        //测试2：测试use 切换库是否能切换到有权限访问的库
        try {
            spark.sql("use mytest1")
        } catch {
            case _: Exception => logError(" use mytest1 权限校验失败")
        }
        spark.sql("use datark_dim_test") //权限校验成功
    }

    def test3(spark: SparkSession) = {
        //测试3：测试 是否有权限create | drop databases;
        try {
            spark.sql("create database wsk_test222")
        } catch {
            case _: Exception => logError("create databases 权限校验失败")
        }
        try {
            spark.sql("drop database wsk_test222")
        } catch {
            case _: Exception => logError("drop databases 权限校验失败")
        }
    }

    def test4(spark: SparkSession) = {
        //测试4：测试 show tables 是否只展示有权限访问的表
        spark.sql("show tables").collect().foreach(println(_)) //显示默认default库的所有表
        spark.sql("use datark_dim_test") //切换只有USE权限的datark_dim_test库，展示所有USE或者SELECT权限的表
        spark.sql("show tables").collect().foreach(println(_))
    }

    def test5(spark: SparkSession) = {
        //测试5: 测试是否有权限crteate |drop table
        spark.sql("drop table hr_test.wsk_test20220107001") //删除表
        spark.sql(
            """
              | CREATE TABLE `hr_test`.`wsk_test20220107001`(
              |`id` bigint,
              |`employee_id` bigint,
              |`emps` array<string>,
              |`empss` array<decimal(16,4)>,
              |`info` map<string,string>,
              |`infos` map<string,decimal(16,4)>,
              |`infoss` struct<addr:string,mail:string,sex:string>,
              |`is_no_domestic_residence` string,
              |`person_investment` decimal(16,2),
              |`is_delete` bigint,
              |`modify_date` string)
              |ROW FORMAT SERDE
              |'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
              |STORED AS INPUTFORMAT
              |'org.apache.hadoop.mapred.TextInputFormat'
              |OUTPUTFORMAT
              |'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'""".stripMargin)  //创建表

        try {
            spark.sql("drop table datark_dim_test.oec_lease_empower_info") //显示默认default库的所有表
        } catch {
            case _: Exception => logError("drop table 权限校验失败")
        }

        try {
            spark.sql(
                """
                  |CREATE TABLE `datark_dim_test`.`oec_lease_empower_info`(
                  |  `id` bigint COMMENT '自增主键',
                  |  `customer_id` string COMMENT '客户id',
                  |  `customer_entity_id` string COMMENT '客户认证id',
                  |  `service_code` string COMMENT 'service代码',
                  |  `service_type` string COMMENT '业务类型,01:代账,04:亿企生意',
                  |  `source_type` string COMMENT '来源类型：订单order',
                  |  `source_value` string COMMENT '订单行明细id',
                  |  `original_source_value` string COMMENT '取消、作废、换购订单行明细id',
                  |  `effective_start_date` string COMMENT '授权生效时间起',
                  |  `effective_end_date` string COMMENT '授权生效时间止',
                  |  `service_value` bigint COMMENT 'service数量',
                  |  `status` string COMMENT '状态:1有效2无效',
                  |  `create_time` string COMMENT '创建时间',
                  |  `last_modified` string COMMENT '最后修改时间',
                  |  `available` bigint COMMENT '逻辑删除标志')
                  |COMMENT '包年周期授权明细'
                  |PARTITIONED BY (
                  |  `pt_d` string COMMENT '天分区')
                  |ROW FORMAT SERDE
                  |  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
                  |STORED AS INPUTFORMAT
                  |  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
                  |OUTPUTFORMAT
                  |  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
                  |LOCATION
                  |  'hdfs://nameHAservice/user/hive/warehouse/datark_dim_test.db/oec_lease_empower_info'
                  |TBLPROPERTIES (
                  |  'transient_lastDdlTime'='1617015060') """.stripMargin) //切换只有USE权限的datark_dim_test库，展示所有USE或者SELECT权限的表
        } catch {
            case _: Exception => logError("create table 权限校验失败")
        }
    }

    def test6(spark: SparkSession) = {
        //测试6: 测试是否有敏感字段权限
        spark.sql("select * from hr_test.wsk_test20220107001").collect().foreach(println(_)) //select * 查询有库ALL权限的表
        spark.sql("select * from  datark_dim_test.oec_lease_empower_info where pt_d = '2022-06-20' and id > 111 and substr(customer_id,3) = '123' ").collect().foreach(println(_)) ///select * 查询只有SELECT权限的表
        try {
            spark.sql("select * from  datark_dim_test.liyang where pt_d = '2022-06-20'").collect().foreach(println(_)) ///select * 查询只有部分字段权限的表
        } catch {
            case _: Exception => logError("select * table 权限校验失败")
        }
    }

    def test8(spark: SparkSession) = {

//        spark.sql("select * from hr_test.wsk_test20220107001").collect().foreach(println(_))
//        spark.sql("INSERT into TABLE `hr_test`.`wsk_test20220107002` SELECT 1,1,1,1")
        //测试8: 测试是否具有create｜ drop view的权限
//        spark.sql("select * from hr_test.wsk_test20220107001_v2").collect().foreach(println(_))
//        spark.sql("DROP VIEW hr_test.wsk_test20220107001_v2")
        spark.sql(
            """
              |create view if not exists hr_test.wsk_test20220107001_v2
              |as
              |select * from hr_test.wsk_test20220107001
              |""".stripMargin) //在有ALL权限的库进行create view
        spark.sql("drop view hr_test.wsk_test20220107001_v2") //在有ALL权限的库进行drop view
        try {
            spark.sql(
                """
                  |create view if not exists hive_test.wsk_test20220107001_v999
                  |as
                  |select * from hr_test.wsk_test20220107001
                  |""".stripMargin) //读取ALL的表create view注册到无ALL权限的库
        } catch {
            case e: Exception => logError("create view 权限校验失败")
        }

        try {
            spark.sql(
                """
                  |create view if not exists hive_test.liyang_v
                  |as
                  |select * from datark_dim_test.liyang
                  |""".stripMargin) //读取无SELECT权限的表create view注册到无ALL权限的库
        } catch {
            case _: Exception => logError("create view 权限校验失败")
        }
    }


    def test7(spark: SparkSession) = {
        //测试8:create table as 的权限
        spark.sql(
            """
              |create table hr_test.wsk_test20220107001_t
              |as
              |select * from hr_test.wsk_test20220107001
              |""".stripMargin) //在有ALL权限的库进行create table
        spark.sql("drop table hr_test.wsk_test20220107001_t") //在有ALL权限的库进行drop table
        try {
            spark.sql(
                """
                  |create table datark_dim_test.wsk_test20220107001_t
                  |as
                  |select * from hr_test.wsk_test20220107001
                  |""".stripMargin) //读取ALL的表create table注册到无ALL权限的库
        } catch {
            case _: Exception => logError("create table 权限校验失败")
        }

        try {
            spark.sql(
                """
                  |create table datark_dim_test.wsk_test20220107001_001
                  |stored as parquet as
                  |select * from hr_test.wsk_test20220107001
                  |""".stripMargin) //读取ALL的表create table stored as parquet as 注册到无ALL权限的库
        } catch {
            case _: Exception => logError("create table 权限校验失败")
        }

        try {
            spark.sql(
                """
                  |create table datark_dim_test.liyang_t
                  |as
                  |select * from datark_dim_test.liyang
                  |""".stripMargin) //读取无SELECT权限的表create table注册到无ALL权限的库
        } catch {
            case _: Exception => logError("create table 权限校验失败")
        }
    }

    def test9(spark: SparkSession) = {
        //测试9:SELECT * ViEW的权限
        try {
            spark.sql("select * from  datark_dim_test.liyang_v").collect().foreach(println(_)) //读取ALL的表create table注册到无ALL权限的库
        } catch {
            case _: Exception => logError("create table 权限校验失败")
        }
    }

    def test(spark: SparkSession) = {
        //测试6: 测试是否有敏感字段权限
        spark.sql("select * from hr_test.wsk_test20220107001").collect().foreach(println(_)) //select * 查询有库ALL权限的表
        spark.sql("select * from  datark_dim_test.oec_lease_empower_info where pt_d = '2022-06-20' ").collect().foreach(println(_)) ///select * 查询只有SELECT权限的表
        try {
            spark.sql("select * from  datark_dim_test.liyang where pt_d = '2022-06-20'").collect().foreach(println(_)) ///select * 查询只有部分字段权限的表
        } catch {
            case _: Exception => logError("select * table 权限校验失败")
        }
    }

    def test15(spark: SparkSession) = {
        //测试15: concat 函数解析测试
        try {
            spark.sql("select concat(pt_d,concat(id,'2'),account_id,'1') from  datark_dim_test.liyang where pt_d = '2022-06-20'").collect().foreach(println(_)) ///select * 查询只有部分字段权限的表
            spark.sql("select sum(concat(pt_d,'1')) from  datark_dim_test.liyang where pt_d = '2022-06-20'").collect().foreach(println(_)) ///select * 查询只有部分字段权限的表

        } catch {
            case _: Exception => logError("concat 函数解析测试失败")
        }
    }

    def test16(spark: SparkSession) = {
        //测试16: 子查询 字段解析 + where 过滤测试 以及 order by测试
        try {
            spark.sql(
                """
                  |SELECT
                  |t.sbuuid AS SBUUID,
                  |t.SKSSNY,
                  |t1.fdxmzdzwm AS ZZSJYJSFWBDCWXZCPDYZL
                  |FROM (SELECT sbuuid AS SBUUID,
                  |concat_ws('-',substr(skssny,1,2),substr(skssny,3,2)) AS SKSSNY,
                  |'1' AS NUM
                  |FROM hive_test.sb_zzs_ybnsr
                  |WHERE sbuuid = '2022-05-11' order by uuid,pt_d) t
                  |LEFT JOIN (SELECT *,'1' AS NUM FROM hive_test.wd_zzsfdxm WHERE pt_d = '2022-05-11') t1
                  |ON t.num = t1.num
                  |""".stripMargin).collect().foreach(println(_)) ///select * 查询只有部分字段权限的表

        } catch {
            case _: Exception => logError("子查询 字段解析测试权限校验失败")
        }
    }

    def test17(spark: SparkSession) = {
        //测试17: 校验字段级权限进行， count(1),count(*),count(字段)
        try {
//            spark.sql(""" select count(2) from  datark_dim_test.liyang""".stripMargin).collect().foreach(println(_))
//            spark.sql(""" select count(*) from  datark_dim_test.liyang""".stripMargin).collect().foreach(println(_))
//           spark.sql(""" select count(id) from  datark_dim_test.liyang""".stripMargin).collect().foreach(println(_))
            spark.sql(""" select count(account_id) from  datark_dim_test.liyang""".stripMargin).collect().foreach(println(_))
        } catch {
            case _: Exception => logError("子查询 字段解析测试权限校验失败")
        }
    }

    def test18(spark: SparkSession) = {
        //测试18: insert overwrite语法权限校验
        try {
            spark.sql(
                """INSERT overwrite table hive_test.sq_test2103_06
                  |SELECT * from datark_dim_test.sq_test2103_06""".stripMargin)
        } catch {
            case _: Exception => logError("子查询 字段解析测试权限校验失败")
        }
    }

    def test19(spark: SparkSession) = {
        //测试19: 临时视图创建以及查询权限校验
        try {
            spark.sql(
                """CREATE TEMPORARY VIEW aiii_4_view  as
                  |select * from datark_dim_test.liyang
                  |""".stripMargin)
        } catch {
            case _: Exception => logError("权限校验失败")
        }

        try {
            spark.sql(
                """select * from aiii_4_view""".stripMargin).collect().foreach(println(_))
        } catch {
            case _: Exception => logError("权限校验失败")
        }
    }

    def test20(spark: SparkSession) = {
        //测试19: 临时视图操作es索引
        spark.sql("set spark.sql.datetime.java8API.enabled = false")
        try {
            spark.sql(
                """CREATE TEMPORARY VIEW aiii_4 USING org.elasticsearch.spark.sql OPTIONS (
                  |  resource 'mysearch-mirror-ads_iii_dev-v3/_doc',
                  |  nodes '10.199.151.14',
                  |  port '9200',
                  |  net.http.auth.user 'mysearch',
                  |  net.http.auth.pass 'mysearch_es_pass',
                  |  net.http.auth.pass.encrypted 'true',
                  |  field.read.empty.as.null 'no',
                  |  security.vault.appcode 'datark',
                  |  security.vault.gateway.url 'http://jupiter-gateway.servyou-stable.sit.91lyd.com',
                  |  security.vault.appkey 'NTBENDA1ODc4MTNFNDlFRkE1QUJEMTgyNjlFOTM5Rjc=',
                  |  security.vault.appsecret 'yihh+ahidJSH4gT0mUMpZw=='
                  |)
                  |""".stripMargin)
        } catch {
            case _: Exception => logError("权限校验失败")
        }

        try {
            spark.sql(
                """CREATE TEMPORARY VIEW aiii_4_view  as
                  |select * from
                  |   (select * ,row_number() over( partition by `institution_id`,`institution_type` order by `@mt` desc) as rowNumber  from aiii_4 where `@del` = false) z where z.rowNumber <= 1""".stripMargin)
        } catch {
            case _: Exception => logError("权限校验失败")
        }

        try {
            spark.sql(
                """CREATE TEMPORARY VIEW mcbbi_3 USING org.elasticsearch.spark.sql OPTIONS (
                  |  resource 'mysearch-mirror-md_cbb_info_dev-v6/_doc',
                  |  nodes '10.199.151.14',
                  |  port '9200',
                  |  net.http.auth.user 'mysearch',
                  |  net.http.auth.pass 'mysearch_es_pass',
                  |  net.http.auth.pass.encrypted 'true',
                  |  field.read.empty.as.null 'no',
                  |  security.vault.appcode 'datark',
                  |  security.vault.gateway.url 'http://jupiter-gateway.servyou-stable.sit.91lyd.com',
                  |  security.vault.appkey 'NTBENDA1ODc4MTNFNDlFRkE1QUJEMTgyNjlFOTM5Rjc=',
                  |  security.vault.appsecret 'yihh+ahidJSH4gT0mUMpZw=='
                  |)""".stripMargin)
        } catch {
            case _: Exception => logError("权限校验失败")
        }

        try {
            spark.sql(
                """CREATE TEMPORARY VIEW mcbbi_3_view as
                  |select *  from mcbbi_3 where `@del` = false""".stripMargin)
        } catch {
            case _: Exception => logError("权限校验失败")
        }

        try {
            spark.sql(
                """CREATE TEMPORARY VIEW scene(
                  |  `mcbbi_3@@id` string,
                  |  `mcbbi_3@@del` string,
                  |  `mcbbi_3@@mt` timestamp,
                  |  `mcbbi_3@id` long,
                  |  `mcbbi_3@customer_id` string,
                  |  `mcbbi_3@customer_type` string,
                  |  `mcbbi_3@create_time` timestamp,
                  |  `mcbbi_3@operator` string,
                  |  `mcbbi_3@business_belong` string,
                  |  `mcbbi_3@task_json` string,
                  |  `mcbbi_3@operator_de` string,
                  |  `mcbbi_3@operator_low_case` string,
                  |  `mcbbi_3@task_json_alias` string,
                  |  `@st@mcbbi_3` string,
                  |  `aiii_4@@id` string,
                  |  `aiii_4@@del` string,
                  |  `aiii_4@@mt` timestamp,
                  |  `aiii_4@id` long,
                  |  `aiii_4@institution_id` string,
                  |  `aiii_4@institution_type` string,
                  |  `aiii_4@mobile` string,
                  |  `aiii_4@create_time` timestamp,
                  |  `aiii_4@renewal_rate` double,
                  |  `aiii_4@mobile_en` string,
                  |  `@st@aiii_4` string,
                  |  `@timestamp` timestamp
                  |) USING org.elasticsearch.spark.sql OPTIONS (
                  |  resource 'mysearch-scene-multi_join_wsk_test_0407001-v17/_doc',
                  |  es.mapping.id 'mcbbi_3@@id',
                  |  nodes '10.199.151.14',
                  |  port '9200',
                  |  net.http.auth.user 'mysearch',
                  |  net.http.auth.pass 'mysearch_es_pass',
                  |  net.http.auth.pass.encrypted 'true',
                  |  field.read.empty.as.null 'no',
                  |  security.vault.appcode 'datark',
                  |  security.vault.gateway.url 'http://jupiter-gateway.servyou-stable.sit.91lyd.com',
                  |  security.vault.appkey 'NTBENDA1ODc4MTNFNDlFRkE1QUJEMTgyNjlFOTM5Rjc=',
                  |  security.vault.appsecret 'yihh+ahidJSH4gT0mUMpZw=='
                  |);""".stripMargin)
        } catch {
            case _: Exception => logError("权限校验失败")
        }

        try {
            spark.sql(
                """insert into
                  |  scene
                  |select
                  |  /*+ REPARTITION(5, d_id) */
                  |  cast(mcbbi_3.`@id`  as string)  as d_id,
                  |  cast(mcbbi_3.`@del`  as string),
                  |  cast(mcbbi_3.`@mt`  as timestamp),
                  |  cast(get_json_object(string(mcbbi_3.`@data`), '$.id') as long) ,
                  |  cast(get_json_object(string(mcbbi_3.`@data`), '$.customer_id') as string) ,
                  |  cast(get_json_object(string(mcbbi_3.`@data`), '$.customer_type') as string) ,
                  |  timestamp_millis(cast(get_json_object(string(mcbbi_3.`@data`), '$.create_time') as long)) ,
                  |  cast(get_json_object(string(mcbbi_3.`@data`), '$.operator') as string) ,
                  |  cast(get_json_object(string(mcbbi_3.`@data`), '$.business_belong') as string) ,
                  |  cast(get_json_object(string(mcbbi_3.`@data`), '$.task_json') as string) ,
                  |  '1' ,
                  |  '1' ,
                  |  '1',
                  |  if(mcbbi_3.`@id` IS NULL, 'NULL', 'LATEST'),
                  |  cast(aiii_4.`@id`  as string),
                  |  cast(aiii_4.`@del`  as string),
                  |  cast(aiii_4.`@mt`  as timestamp),
                  |  cast(get_json_object(string(aiii_4.`@data`), '$.id') as long) ,
                  |  cast(get_json_object(string(aiii_4.`@data`), '$.institution_id') as string) ,
                  |  cast(get_json_object(string(aiii_4.`@data`), '$.institution_type') as string) ,
                  |  cast(get_json_object(string(aiii_4.`@data`), '$.mobile') as string) ,
                  |  timestamp_millis(cast(get_json_object(string(aiii_4.`@data`), '$.create_time') as long)) ,
                  |  cast(get_json_object(string(aiii_4.`@data`), '$.renewal_rate') as double) ,
                  |  '1' ,
                  |  if(aiii_4.`@id` IS NULL, 'NULL', 'LATEST'),
                  |  now()
                  |from
                  |  mcbbi_3_view mcbbi_3
                  |  left join aiii_4_view aiii_4 on mcbbi_3.`customer_id` = aiii_4.`institution_id` and mcbbi_3.`customer_type` = aiii_4.`institution_type`""".stripMargin)
        } catch {
            case _: Exception => logError("权限校验失败")
        }
    }

    def test21(spark: SparkSession) = {
        //测试21:操作hudi权限校验
        spark.sql("set hoodie.sql.bulk.insert.enable=true")
        spark.sql("set hoodie.sql.insert.mode=non-strict")
        try {
            spark.sql(
                """CREATE TABLE IF NOT EXISTS `servyou_ods`.`hudi_ds_test4_pri_wsk_20220707003`
                  |(
                  |`id` bigint
                  |, `task_id` bigint
                  |, `row_data` string
                  |, `row_index` bigint
                  |, `record_state` string
                  |, `record_remark` string
                  |, `modify_time` string
                  |, `create_time` string
                  |, `create_user` string
                  |, `ts` string -- binlog时间戳
                  |--, `pt_m` string -- 分区字段,格式yyyyMM，非分区表请注释
                  |) USING hudi options (
                  |primaryKey = 'id' -- 主键
                  |,preCombineField = 'ts' -- 去重键
                  |,type = 'cow'
                  |--,hoodie.datasource.write.hive_style_partitioning='true' -- Hive风格分区
                  |--,hoodie.datasource.hive_sync.partition_fields='pt_m' -- 分区字段，非分区表请注释
                  |,hoodie.parquet.compression.codec = 'snappy'
                  |,hoodie.table.keygenerator.class='org.apache.hudi.keygen.NonpartitionedKeyGenerator' -- 非分区表键生成器调整成 NonpartitionedKeyGenerator
                  |) """.stripMargin)
        } catch {
            case _: Exception => logError("权限校验失败")
        }
        spark.sql("show tables in servyou_ods").collect().foreach(println(_))
        try {
            spark.sql(
                """INSERT INTO `servyou_ods`.`hudi_ds_test4_pri_wsk_20220707003`
                  |SELECT
                  |`id`
                  |, `task_id`
                  |, `row_data`
                  |, `row_index`
                  |, `record_state`
                  |, `record_remark`
                  |, `modify_time`
                  |, `create_time`
                  |, `create_user`
                  |, from_unixtime(unix_timestamp('2022-06-18', "yyyy-MM-dd"), 'yyyy-MM-dd HH:mm:ss')
                  |FROM hive_test.ds_test4""".stripMargin)
        } catch {
            case _: Exception => logError("权限校验失败")
        }
        try {
            spark.sql("SELECT * from `servyou_ods`.`hudi_ds_test4_pri_wsk_20220707003`  limit 10").collect().foreach(println(_))
        } catch {
            case _: Exception => logError("权限校验失败")
        }
    }

    def test22(spark: SparkSession) = {
        //操作1000列校验，压存储日志接口
        spark.sql("SELECT * from hive_test.test_component").show()
        spark.sql("SELECT * from default.abc").show()
    }

    def test23(spark: SparkSession): Unit = {
        //explain 一会有错一会儿无错报错处理,问题原因是explain 的plain的底层的表未解析,导致解析过滤字段toSeq时报错
//        spark.sql("EXPLAIN SELECT * from servyou_tmp.hudi_ds_test4_copy1 where pt_d ='2022-07-06' and task_id=1;").show(false)
        try {
            spark.sql(
                """
                  |explain insert overwrite table xqy_period.rep_d_iit_pts_income_details
                  |select distinct s.id, s.income_item_code, s.income_item_name, s.customer_id, s.employee_id, s.declaration_id, s.type, from_unixtime(unix_timestamp()) create_date, dept_id, income
                  |  from (select id, income_item_code, income_item_name, customer_id, employee_id, declaration_id, '综合所得' as type, dept_id, income
                  |          from servyou_ods.ods_edw010_iit_complex_income_df
                  |                 where is_delete = 0 and `pt_d` = '2022-7-12'
                  |         union all
                  |        select id, income_item_code, income_item_name, customer_id, employee_id, declaration_id, '分类所得' as type, dept_id, income
                  |          from servyou_ods.ods_edw010_iit_classification_income_df
                  |                 where is_delete = 0 and `pt_d` = '2022-7-12'
                  |                 union all
                  |        select id, income_item_code, income_item_name, customer_id, employee_id, declaration_id, '非居民所得' as type, dept_id, income
                  |          from servyou_ods.ods_edw010_iit_non_residents_income_df
                  |                 where is_delete = 0 and `pt_d` = '2022-7-12')s;
                  |""".stripMargin).show(false)
        } catch {
            case _: Exception => logError("权限校验失败")
        }
    }

    def test24(spark: SparkSession): Unit = {
        //select '1''23' 未报错校验,看plan是自动合在在一起
        spark.sql("select '1''123'").show(false)
    }

    def test25(spark: SparkSession): Unit = {
        //两个max(时间函数)导致long overflow异常
        spark.sql("select count(1) from servyou_tmp.tmp_event_warden_log_mild_parse_hi_wsk_test2 t;").show(false)
        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY;") //默认值是EXCEPTION,此值不会造成long overflow
        try {
            spark.sql(
                """
                  |SELECT
                  |  max(case when name = 'starttime' then from_unixtime(cast(if(value in ('', 'null'), null, value)  as bigint),'yyyy-MM-dd HH:mm:ss') end) as start_time,
                  |  max(case when name = 'endtime' then from_unixtime(cast(if(value in ('', 'null'), null, value)  as bigint),'yyyy-MM-dd HH:mm:ss') end) as end_time
                  |from
                  |    servyou_tmp.tmp_event_warden_log_mild_parse_hi_wsk_test2 t
                  |where pt_d = '2022-08-31' limit 1000 ;
                  |""".stripMargin).show(false)
        } catch {
            case e: Throwable => logError("异常", e)
        }
    }

    def test26(spark: SparkSession) = {
        //测试26: 测试是否具有默认all权限库的 create｜ drop table的权限
        spark.sql(
            """
              |create table if not exists default.wsk_test20220107001_v2
              |as
              |select * from hr_test.wsk_test20220107001
              |""".stripMargin) //在有ALL权限的库进行create view
        spark.sql("drop table default.wsk_test20220107001_v2") //在有ALL权限的库进行drop view
        try {
            spark.sql(
                """
                  |create table if not exists datark_query_download_temp.wsk_test20220107001_v999
                  |as
                  |select * from hr_test.wsk_test20220107001
                  |""".stripMargin) //读取ALL的表create view注册到无ALL权限的库
        } catch {
            case e: Exception => logError("create view 权限校验失败")
        }
    }

    def test27(spark: SparkSession) = {
        //测试27: 测试DCL,查询所有hive角色
        try {
            spark.sql("show roles").show(false)
        } catch {
            case e: Exception => logError("查询所有hive角色失败")
        }
    }

    def test28(spark: SparkSession) = {
        //测试27: 测试DCL,查询所有hive角色
        try {
            spark.sql("set spark.sql.storeAssignmentPolicy=legacy;")
            spark.sql("""
                        |INSERT OVERWRITE TABLE zr_dev.TMP_MX_YH_JMYHYXSGJMX_LSB_TSZD_JCZD_QSYJHJ_wsk_test PARTITION(PT_D='2023-02-05', SJLYBBM='MX_YH_JMYHYXSGJMX.4111')
                        |SELECT
                        |
                        |    t.cpkhdah
                        |    ,t.sbbid
                        |    ,t.sbuuid
                        |    ,t.skssny
                        |    ,t.lrrq
                        |    ,t.xgrq
                        |    ,'1' as uuid
                        |    ,t.DZBZDSZL_DM
                        |    ,t.SBZBDZBZDSZL_DM
                        |    ,t.ZSFS_DM
                        |    ,'1' as JMSWSX_DM
                        |    ,'1' as SSJMXZ_DM
                        |    ,t.JMLX_DM
                        |    ,'1' as JMYSX
                        |    ,case when '1' in ('10','11','34','35','36','37') then 0 else '1' end as SRYHJE
                        |    ,case when '1' in ('10','11','34','35','36','37') then '1' else 0 end as KCYHJE
                        |    ,null as YHSL
                        |    ,'1' as ZSJMSE   -- NVL（本表的{减免应税项}，0）*0.25
                        |    ,'1' as JMSE     -- 本表的{折算减免税额}
                        |    ,'1' as SFJMSEZSBZ
                        |    ,'1' as YWGCLYBM
                        |    ,'1' as YWGCMC
                        |    ,'41' AS SBSX_DM_1
                        |    ,substring(t.skssny,1,4) as skssnf
                        |FROM
                        |     zr_dev.TMP_MX_YH_JMYHYXSGJMX_LSB_TSZD_JCZD_QSYJHJ_wsk_test_2 t;
                        |     """.stripMargin).show(false)
        } catch {
            case e: Exception => logError("")
        }
    }

    def test29(spark: SparkSession) = {
        //测试29: 测试spark 血缘插件
        try {
            spark.sql("set spark.sql.storeAssignmentPolicy=legacy;")
            spark.sql(
                """
                  |INSERT OVERWRITE TABLE zr_dev.TMP_MX_YH_JMYHYXSGJMX_LSB_TSZD_JCZD_QSYJHJ_wsk_test PARTITION(PT_D='2023-02-05', SJLYBBM='MX_YH_JMYHYXSGJMX')
                  |SELECT
                  |
                  |    t.cpkhdah
                  |    ,t.sbbid
                  |    ,t.sbuuid
                  |    ,t.skssny
                  |    ,t.lrrq
                  |    ,t.xgrq
                  |    ,'1' as uuid
                  |    ,t.DZBZDSZL_DM
                  |    ,t.SBZBDZBZDSZL_DM
                  |    ,t.ZSFS_DM
                  |    ,'1' as JMSWSX_DM
                  |    ,'1' as SSJMXZ_DM
                  |    ,t.JMLX_DM
                  |    ,'1' as JMYSX
                  |    ,case when '1' in ('10','11','34','35','36','37') then 0 else '1' end as SRYHJE
                  |    ,case when '1' in ('10','11','34','35','36','37') then '1' else 0 end as KCYHJE
                  |    ,null as YHSL
                  |    ,'1' as ZSJMSE   -- NVL（本表的{减免应税项}，0）*0.25
                  |    ,'1' as JMSE     -- 本表的{折算减免税额}
                  |    ,'1' as SFJMSEZSBZ
                  |    ,'1' as YWGCLYBM
                  |    ,'1' as YWGCMC
                  |    ,'41' AS SBSX_DM_1
                  |    ,substring(t.skssny,1,4) as skssnf
                  |FROM
                  |     zr_dev.TMP_MX_YH_JMYHYXSGJMX_LSB_TSZD_JCZD_QSYJHJ_wsk_test_2 t;
                  |     """.stripMargin).show(false)
            Thread.sleep(5000)
        } catch {
            case e: Exception => logError("发生异常",e)
        }
    }

    def test30(spark: SparkSession) = {
        //测试30: 测试 join、where输入表级别血缘丢失问题
        try {
            spark.sql(
                """
                  |
                  |insert overwrite table dev_datalineage.test_where_field
                  |select
                  |       a.field1,
                  |       a.field2,
                  |       a.field3
                  |from dev_datalineage.join_table_a a
                  |LEFT JOIN dev_datalineage.join_table_b b on a.field1 = b.field1
                  |LEFT JOIN dev_datalineage.join_table_c c on a.field1 = c.field1
                  |where c.field2='2'
                  |""".stripMargin).show(false)
            Thread.sleep(5000)
        } catch {
            case e: Exception => logError("发生异常", e)
        }
    }

    def test31(spark: SparkSession) = {
        //测试31: 测试 行级别过滤
//        // 查询 select
//        try {
//            val df = spark.sql(
//                """
//                  |
//                  |select count(id) from datark_dev.wsk_table_row_filter_test;
//                  |
//                  |""".stripMargin)
//            println(df.queryExecution.optimizedPlan)
//            df.show()
//            Thread.sleep(5000)
//        } catch {
//            case e: Exception => logError("发生异常", e)
//        }

////        //插入数据 INSERT OVERWRITE
//        try {
//            val df = spark.sql(
//                """
//                  |
//                  |INSERT OVERWRITE  `hr_test`.wsk_test_row_fileter2
//                  |SELECT *  from `hr_test`.`wsk_test_row_fileter`
//                  |where employee_id >=2 AND employee_id< 6
//                  |
//                  |""".stripMargin)
//            println(df.queryExecution.optimizedPlan)
//            df.show()
//            Thread.sleep(5000)
//        } catch {
//            case e: Exception => logError("发生异常", e)
//        }
//
//        //插入数据 INSERT INTO
//        try {
//            spark.sql("TRUNCATE TABLE `hr_test`.`wsk_test_row_fileter2`")
//            val df = spark.sql(
//                """
//                  |
//                  |INSERT INTO TABLE `hr_test`.wsk_test_row_fileter2
//                  |SELECT *
//                  |from `hr_test`.`wsk_test_row_fileter` where employee_id >= 2 AND employee_id < 6;
//                  |
//                  |""".stripMargin)
//            println(df.queryExecution.optimizedPlan)
//            Thread.sleep(5000)
//        } catch {
//            case e: Exception => logError("发生异常", e)
//        }
//
//
//        //插入数据 CREATE TABLE as select
//        try {
//            spark.sql("DROP TABLE if exists `hr_test`.wsk_test_row_fileter3;")
//            val df = spark.sql(
//                """
//                  |
//                  |CREATE TABLE `hr_test`.wsk_test_row_fileter3 as SELECT *  from `hr_test`.`wsk_test_row_fileter` where employee_id >=2 AND employee_id< 6
//                  |
//                  |""".stripMargin)
//            println(df.queryExecution.optimizedPlan)
//            Thread.sleep(5000)
//        } catch {
//            case e: Exception => logError("发生异常", e)
//        }

        //插入数据 CREATE TABLE as select  注意包裹了一层select * 导致走了OptimizedCreateHiveTableAsSelectCommand而非CreateHiveTableAsSelectCommand命令
//        try {
//            spark.sql("DROP TABLE if exists datark_query_download_temp.wsktest;")
//            val df = spark.sql(
//                """
//                  |
//                  |create table datark_query_download_temp.wsktest stored
//                  | as orc as
//                  |select * from (select * from datark_dev.wsk_table_row_filter_test) query_export_datarkalias limit 500000;
//                  |
//                  |""".stripMargin)
//            println(df.queryExecution.optimizedPlan)
//            Thread.sleep(5000)
//        } catch {
//            case e: Exception => logError("发生异常", e)
//        }

        try {
            spark.sql(
                """
                  |
                  | SELECT * from datark_dev.wsk_table_row_filter_test;
                  |
                  |""".stripMargin).show(1000)
            Thread.sleep(5000)
        } catch {
            case e: Exception => logError("发生异常", e)
        }
    }
    def test32(spark: SparkSession) = {
        //测试32: 测试 字段和表混乱
        try {
//            spark.sql("drop table if exists default.itp_message_detail_join_cc_wsk_test;")
            val df = spark.sql(
                """
                  |
                  |select a.*,
                  |    b.full_name,
                  |    b.national_tax_registration_no,
                  |    b.local_tax_registration_no,
                  |    b.national_tax_bureau_code,
                  |    b.local_tax_bureau_code,
                  |    substr(b.biz_region_code,0,2) as province_code,
                  |    substr(b.biz_region_code,0,4) as city_code,
                  |    b.area_id,
                  |    b.biz_region_code,
                  |    b.tax_region_code,
                  |    b.industry_top_category,
                  |    b.industry_main_category,
                  |    b.industry_category,
                  |    b.vat_taxpayer_type
                  |from (SELECT * from hr_test.ods_edw075_itp_message_detail_df where pt_d='123')  as a
                  |LEFT JOIN
                  |(select * from hr_test.ods_edw001_cc_group_profile_info_df where pt_d='123') as b
                  |on b.id = a.company_id

                  |
                  |""".stripMargin)
            df.show()
            Thread.sleep(5000)
        } catch {
            case e: Exception => logError("发生异常", e)
        }
    }

    def test33(spark: SparkSession) = {
        //测试33: 只有select权限的表却能插入数据
        try {
            val df = spark.sql(
                """
                  |
                  |select * from default.mx_fp_fpsphfwxxgjmx_wsk_test ORDER BY XSFDZDH desc;
                  |
                  |""".stripMargin)
            df.show()
            Thread.sleep(5000)
        } catch {
            case e: Exception => logError("发生异常", e)
        }
    }

    def test34(spark: SparkSession) = {
        //测试34: orc snappy文件无法解压问题分析,最终定位 表创建时未指定文件存储格式，使用默认的Text存储，最终生成的的是InsertIntoHiveTable Command，而使用orc存储的表生成的是InsertIntoHadoopFsRelationCommand
        try {
            val df = spark.sql(
                """
                  |
                  |INSERT into datark_dwd_test.sys_user_bak
                  |SELECT
                  | id
                  |,user_name
                  |,user_password
                  |,user_zh_name
                  |,user_type
                  |,email
                  |,phone
                  |,tenant_id
                  |,create_time
                  |,update_time
                  |,queue
                  |,pt_d
                  | from default.sys_user_bak2;
                  |
                  |""".stripMargin)
            df.show()
            Thread.sleep(5000)
        } catch {
            case e: Exception => logError("发生异常", e)
        }
    }

    def test35(spark: SparkSession) = {
        //测试35: 只有select权限的表却能插入数据
        try {
            val df = spark.sql(
                """
                  |
                  |INSERT overwrite table wsk_test.mid_gz_pain_spot select name1,name2,code2,db_name FROM xuehui.zjl_test1;
                  |
                  |""".stripMargin)
            df.show()
            Thread.sleep(5000)
        } catch {
            case e: Exception => logError("发生异常", e)
        }
    }

    def test36(spark: SparkSession) = {
        //测试36：task超时监控
        try {
            val df = spark.sql(
                """
                  |
                  |INSERT INTO hive_test.wsk_pt_m_lifecycle_test8 SELECT * from hive_test.wsk_pt_m_lifecycle_test7 limit 100;
                  |
                  |""".stripMargin)
            df.show()
            Thread.sleep(5000)
        } catch {
            case e: Exception => logError("发生异常", e)
        }
    }

    def test37(spark: SparkSession) = {
        //测试37：两次drop第一次拒绝，第二次成功问题分析定位
        try {
            spark.sql(
                """
                  |
                  |drop table zr_dev.wsk_test0001;;
                  |
                  |""".stripMargin)
            Thread.sleep(5000)
        } catch {
            case e: Exception => logError("发生异常", e)
        }

        try {
            println("sql2")
            spark.sql(
                """
                  |
                  |drop table zr_dev.wsk_test0001;;
                  |
                  |""".stripMargin)
            Thread.sleep(5000)
        } catch {
            case e: Exception => logError("发生异常", e)
        }
    }


    def test38(spark: SparkSession) = {
        //测试38：生产orc文件无法正确读取
        try {
            spark.sql(
                """
                  |
                  |SELECT * from `hive_test`.`mx_fp_fpxxgjmx`  ORDER BY xsfcpkhdah LIMIT 1000;
                  |
                  |
                  |""".stripMargin).show(1000)
            Thread.sleep(5000)
        } catch {
            case e: Exception => logError("发生异常", e)
        }
    }

}
