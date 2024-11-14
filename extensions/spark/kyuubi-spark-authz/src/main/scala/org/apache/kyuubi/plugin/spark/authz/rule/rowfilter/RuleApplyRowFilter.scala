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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.kyuubi.plugin.spark.authz.ObjectType
import org.apache.kyuubi.plugin.spark.authz.OperationType.QUERY
import org.apache.kyuubi.plugin.spark.authz.ranger.SparkRangerAdminPlugin.getConfig
import org.apache.kyuubi.plugin.spark.authz.ranger._
import org.apache.kyuubi.plugin.spark.authz.rule.RuleHelper
import org.apache.kyuubi.plugin.spark.authz.security.DatarkSparkAuthentication
import org.apache.kyuubi.plugin.spark.authz.serde._

case class RuleApplyRowFilter(spark: SparkSession) extends RuleHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (rowFilterEnabled()) {
      val newPlan = mapChildren(plan) {
        case p: RowFilterMarker => p
        case scan if isKnownScan(scan) && scan.resolved =>
          val tables = getScanSpec(scan).tables(scan, spark)
          tables.headOption.map(applyFilter2(scan, _)).getOrElse(scan)
        case other => apply(other)
      }
      newPlan
    } else {
      plan
    }
  }

  private def applyFilter(
      plan: LogicalPlan,
      table: Table): LogicalPlan = {
    val are = AccessResource(ObjectType.TABLE, table.database.orNull, table.table, null)
    val art = AccessRequest(are, ugi, QUERY, AccessType.SELECT)
    val filterExpr = SparkRangerAdminPlugin.getFilterExpr(art).map(parse)
    val filtered = filterExpr.foldLeft(plan)((p, expr) => Filter(expr, RowFilterMarker(p)))
    filtered
  }

  private def applyFilter2(
                                  plan: LogicalPlan,
                                  table: Table): LogicalPlan = {
    val (userName, datarkUrl, appCode, expireTime, _, _, _, projectCode, _) = getConfig(spark)
    val tableDbName: String = table.database.getOrElse("default") + "." + table.table
    val filterExprStr = DatarkSparkAuthentication.getTableRowFilterExp(userName, appCode, datarkUrl, expireTime, projectCode, tableDbName)
    val filterExpr = Option(filterExprStr).filter(fe => fe != null && fe.nonEmpty).map(parse)
    val filtered = filterExpr.foldLeft(plan)((p, expr) => Filter(expr, RowFilterMarker(p)))
    filtered
  }

  private def rowFilterEnabled(): Boolean = "true".equalsIgnoreCase(spark.sparkContext.getConf.get("spark.3.4.3.datark.security.authorization.rowFilter.enable", "false"))

}
