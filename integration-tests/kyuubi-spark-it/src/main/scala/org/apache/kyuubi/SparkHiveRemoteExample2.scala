/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

// scalastyle:off
object SparkHiveRemoteExample2 extends Logging {


    // $example on:spark_hive$
    case class Record(key: Int, value: String)
    // $example off:spark_hive$

    def main(args: Array[String]) {
        HdfsKerberos.kerberos(new Configuration())
        val spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("Spark Hive Example")
                .config("spark.sql.hive.metastore.version", "1.1.0")
                .config("spark.sql.hive.metastore.jars", "path")
                .config("spark.sql.hive.metastore.jars.path"
                    , "file:///opt/cloudera/parcels/CDH-5.16.1-1.cdh5.16.1.p0.3/lib/hive/lib/*")
                .enableHiveSupport()
                /**
                 * spark 血缘分析测试
                 * 1)开启如下三个配置
                 * 2)将血缘解析插件放入spark的cp
                 * 3)将血缘事件监听的插件放入spark的cp
                 */
                .config("spark.sql.queryExecutionListeners"
                    , "org.apache.kyuubi.plugin.lineage2.SparkOperationLineageQueryExecutionListener")
                .getOrCreate()
        logInfo("这里是测试111")
//      测试1: 测试 join、where输入表级别血缘丢失问题
//      test1(spark)
//      测试2: 测试view输入表级别血缘丢失问题
//        test2(spark)
//      测试3: 更多测试view输入表级别血缘丢失问题
//        test3(spark)
//      测试4:视图中带in子查询，血缘测试
//        test4(spark)
        // 测试5:普通查询带in子查询，血缘测试
//        test5(spark)
        // 测试6：not exists(select 1..) 表级别
//        test6(spark)
        // 测试7：/(select 1..)>  的表级别级别
        test7(spark)
        spark.stop()

    }

    def test1(spark: SparkSession): Unit = {
        // 测试1: 测试 join、where输入表级别血缘丢失问题
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

    def test2(spark: SparkSession): Unit = {
        // 测试2: 测试view输入表级别血缘丢失问题
        try {
            spark.sql(
                """
                  |
                  |insert
                  |	overwrite table zjl_test.kbc_re_yq_user_id_sample
                  |select
                  |		e.account_id,
                  |        '202020' pt_d
                  |from
                  |	(
                  |	select
                  |		mobile
                  |	from
                  |		zjl_test.ads_agent_mobile_relation_hb
                  |
                  |    ) d
                  |    join zjl_test.ads_mobile_account_relation_base e on
                  |	    d.mobile = e.mobile;
                  |
                  |""".stripMargin).show(false)
            Thread.sleep(5000)
        } catch {
            case e: Exception => logError("发生异常", e)
        }
    }

    def test3(spark: SparkSession): Unit = {
        // 测试3: 测试view输入表级别血缘丢失问题
        try {
            spark.sql(
                """
                  |
                  |insert
                  |	overwrite table zjl_test.kbc_re_yq_user_id_sample
                  |select
                  |	distinct account_id,
                  |	'202020' pt_d
                  |from
                  |	(
                  |	select
                  |		account_id
                  |	from
                  |		zjl_test.ads_mobile_consult_level_tag f
                  |union all
                  |	select
                  |		e.account_id
                  |	from
                  |		(
                  |		select
                  |			mobile
                  |		from
                  |			zjl_test.ads_agent_mobile_relation_hb
                  |	    union all
                  |		select
                  |			mobile
                  |		from
                  |			(
                  |			select
                  |				b.mobile
                  |			from
                  |				zjl_test.ads_company_consult_level_tag a
                  |			join zjl_test.ads_mobile_company_relation_base b on
                  |				a.company_id = b.company_id
                  |            ) c
                  |        ) d
                  |	    join zjl_test.ads_mobile_account_relation_base e on
                  |		    d.mobile = e.mobile
                  |) g
                  |
                  |""".stripMargin).show(false)
            Thread.sleep(5000)
        } catch {
            case e: Exception => logError("发生异常", e)
        }
    }

    def test4(spark: SparkSession): Unit = {
        // 测试4：视图中带in子查询，血缘测试
        try {
            spark.sql(
                """
                  |
                  |CREATE TEMPORARY view gov_compute_three_day_error as
                  |SELECT
                  |    task_id,
                  |    project_code,
                  |    collect_date,
                  |    collect_month,
                  |    all_count,
                  |    error_count,
                  |    three_day_error_count,
                  |    number
                  |from (
                  |    SELECT
                  |        task_id,
                  |        project_code,
                  |        collect_date,
                  |        substr(collect_date,1,7) collect_month,
                  |        all_count,
                  |        error_count,
                  |        sum(case when  error_count > 0 then 1 else 0 end  ) OVER(partition by task_id,substr(collect_date,1,7) ORDER BY collect_date ASC  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) three_day_error_count,
                  |        row_number() OVER(partition by task_id,substr(collect_date,1,7) ORDER BY collect_date ASC ) number
                  |    from (
                  |        SELECT
                  |            task_id,
                  |            max(project_code) as project_code,
                  |            substr(schedule_time,1,10) as collect_date,
                  |            count(1) as  all_count,
                  |            sum(case when state = 6 then 1 else 0 end ) as  error_count
                  |        from (
                  |                (select  ts.* from datark_dev.task_schedule_instance ts
                  |                    where ts.instance_type=0 and ts.task_type in ('SQL', 'BATCH_SYNC', 'SPARK_SQL')  and ts.pt_d = '2024-02-18' and ts.task_id in (select task_id from datark_dev.task_schedule where pt_d = '2024-02-18' and task_period = 2)
                  |                ) a
                  |                inner join (select schedule_time,id from datark_dev.task_process_instance where pt_d = '2024-02-18') b on a.process_instance_id = b.id
                  |            ) c group  by task_id,substr(schedule_time,1,10))
                  |) a where a.error_count > 0
                  |
                  |""".stripMargin)

            val df = spark.sql(
                """
                  |
                  |SELECT
                  |        task_id,
                  |        project_code,
                  |        3 as type,
                  |        to_json(named_struct('collectDate', collect_date, 'errorCount', error_count,
                  |            'allCount',all_count)) as detail_jsonl,
                  |        collect_month
                  |    from gov_compute_three_day_error t
                  |
                  |""".stripMargin)
            df.show()
            Thread.sleep(5000)
        } catch {
            case e: Exception => logError("发生异常", e)
        }
    }

    def test5(spark: SparkSession): Unit = {
        // 测试5：普通查询带= >的子查询，血缘测试
        try {
            spark.sql(
                """
                  |
                  |select  ts.* from datark_dev.task_schedule_instance ts
                  |                    where   ts.instance_type=0
                  |                               and ts.task_type in ('SQL', 'BATCH_SYNC', 'SPARK_SQL')
                  |                               and ts.pt_d = '2024-02-18' and ts.task_id = (select task_id from datark_dev.task_schedule where pt_d = '2024-02-18' and task_period = 2 limit 1)
                  |
                  |
                  |""".stripMargin).show()
            Thread.sleep(5000)
        } catch {
            case e: Exception => logError("发生异常", e)
        }
    }

    def test6(spark: SparkSession): Unit = {
        // 测试6：not exists(select 1..) 表级别
        try {
            spark.sql(
                """
                  |
                  |select  ts.* from datark_dev.task_schedule_instance ts
                  |                    where   ts.instance_type=0
                  |                               and ts.task_type in ('SQL', 'BATCH_SYNC', 'SPARK_SQL')
                  |                               and ts.pt_d = '2024-02-18' and not exists (select 1 from datark_dev.task_schedule where pt_d = '2024-02-18' and task_period = 2 limit 1)
                  |
                  |
                  |""".stripMargin).show()
            Thread.sleep(5000)
        } catch {
            case e: Exception => logError("发生异常", e)
        }
    }

    def test7(spark: SparkSession): Unit = {
        // 测试7：/(select 1..)>  的表级别级别
        try {
            spark.sql(
                """
                  |
                  |select  ts.* from datark_dev.task_schedule_instance ts
                  |                    where   ts.instance_type=0
                  |                               and ts.task_type in ('SQL', 'BATCH_SYNC', 'SPARK_SQL')
                  |                               and ts.pt_d = '2024-02-18' and ts.task_id/(select task_id from datark_dev.task_schedule where pt_d = '2024-02-18' and task_period = 2 limit 1) > 1
                  |
                  |
                  |""".stripMargin).show()
            Thread.sleep(5000)
        } catch {
            case e: Exception => logError("发生异常", e)
        }
    }
}
