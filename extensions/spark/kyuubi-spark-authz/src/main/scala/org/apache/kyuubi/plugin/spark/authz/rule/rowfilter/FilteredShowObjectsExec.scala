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
package org.apache.kyuubi.plugin.spark.authz.rule.rowfilter

import org.apache.hadoop.security.UserGroupInformation
import org.apache.kyuubi.plugin.spark.authz.ranger.{AccessResource, SparkRangerAdminPlugin}
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils
import org.apache.kyuubi.plugin.spark.authz.{ObjectType, OperationType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}

trait FilteredShowObjectsExec extends LeafExecNode {
  def result: Array[InternalRow]

  override def output: Seq[Attribute]

  final override def doExecute(): RDD[InternalRow] = {
    sparkContext.parallelize(result, 1)
  }
}

trait FilteredShowObjectsCheck {
  def isAllowed(r: InternalRow, ugi: UserGroupInformation, sparkSession: SparkSession): Boolean
}

case class FilteredShowNamespaceExec(result: Array[InternalRow], output: Seq[Attribute])
  extends FilteredShowObjectsExec {}
object FilteredShowNamespaceExec extends FilteredShowObjectsCheck {
  def apply(delegated: SparkPlan, sc: SparkSession): FilteredShowNamespaceExec = {
    val result = delegated.executeCollect()
      .filter(isAllowed(_, AuthZUtils.getAuthzUgi(sc.sparkContext),sc))
    new FilteredShowNamespaceExec(result, delegated.output)
  }

  override def isAllowed(r: InternalRow, ugi: UserGroupInformation, sparkSession: SparkSession): Boolean = {
    val database = r.getString(0)
    val resource = AccessResource(ObjectType.DATABASE, database, null, null)
    SparkRangerAdminPlugin.isAllowed2(sparkSession, resource, OperationType.SHOWDATABASES)
  }
}

case class FilteredShowTablesExec(result: Array[InternalRow], output: Seq[Attribute])
  extends FilteredShowObjectsExec {}
object FilteredShowTablesExec extends FilteredShowObjectsCheck {
  def apply(delegated: SparkPlan, sc: SparkSession): FilteredShowNamespaceExec = {
    val result = delegated.executeCollect()
      .filter(isAllowed(_, AuthZUtils.getAuthzUgi(sc.sparkContext),sc))
    new FilteredShowNamespaceExec(result, delegated.output)
  }

  override def isAllowed(r: InternalRow, ugi: UserGroupInformation, sparkSession: SparkSession): Boolean = {
    val database = r.getString(0)
    val table = r.getString(1)
    val isTemp = r.getBoolean(2)
    val objectType = if (isTemp) ObjectType.VIEW else ObjectType.TABLE
    val resource = AccessResource(objectType, database, table, null)
    SparkRangerAdminPlugin.isAllowed2(sparkSession, resource, OperationType.SHOWTABLES)
  }
}
