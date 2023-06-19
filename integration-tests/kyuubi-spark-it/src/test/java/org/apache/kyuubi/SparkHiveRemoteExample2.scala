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
                    , "org.apache.kyuubi.plugin.lineage.SparkOperationLineageQueryExecutionListener")
                .getOrCreate()
        logInfo("这里是测试111")
//      测试1: 测试 join、where输入表级别血缘丢失问题
//      test1(spark)
//      测试2: 测试view输入表级别血缘丢失问题
//        test2(spark)
//      测试2: 更多测试view输入表级别血缘丢失问题
        test3(spark)
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
}
