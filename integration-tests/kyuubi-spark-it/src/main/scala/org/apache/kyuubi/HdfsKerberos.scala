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
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
// 关闭代码风格检查
// scalastyle:off
object HdfsKerberos {

    /**
     * 进行hdfs kerberos 登陆验证
     */
    def hdfsKerberos(): Unit = {
        val conf = new Configuration()
        kerberos(conf)

// 需要添加如下两个文件才可访问hdfs，光加HADOOP_HOME以及HADOOP_CONF_DIR是没有用的，
        // 有这两个文件后可不需要HADOOP_HOME以及HADOOP_CONF_DIR的配置
        conf.addResource(new Path("/opt/env/core-site.xml"))
        conf.addResource(new Path("/opt/env/hdfs-site.xml"))
//    访问hdfs代码
        val fs = FileSystem.get(conf)
        fs.globStatus(new Path("/spark/spark3/jars/*")).foreach(x => {
            println(x.getPath)
        })
    }

    /**
     * kerberos 登陆验证
     */
    def kerberos(conf: Configuration): Unit = {

        val user = "hive/hz-hadoop-test-199-151-39@HADOOP.COM"
        val keyPath = "/opt/env/hive.keytab"
        val krb5Path = "/opt/env/krb5.conf"

        System.setProperty("java.security.krb5.conf", krb5Path)

        conf.set("hadoop.security.authentication", "kerberos")
        UserGroupInformation.setConfiguration(conf)
        UserGroupInformation.loginUserFromKeytab(user, keyPath)

    }

    def main(args: Array[String]): Unit = {
        hdfsKerberos()
    }
}
