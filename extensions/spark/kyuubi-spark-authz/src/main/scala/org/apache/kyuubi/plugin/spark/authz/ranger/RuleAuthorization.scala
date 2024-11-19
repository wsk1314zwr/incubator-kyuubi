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

package org.apache.kyuubi.plugin.spark.authz.ranger

import java.util.Locale
import org.apache.commons.logging.LogFactory
import scala.collection.mutable
import org.apache.ranger.plugin.policyengine.RangerAccessRequest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.kyuubi.plugin.spark.authz._
import org.apache.kyuubi.plugin.spark.authz.ObjectType._
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessType.AccessType
import org.apache.kyuubi.plugin.spark.authz.ranger.SparkRangerAdminPlugin._
import org.apache.kyuubi.plugin.spark.authz.rule.Authorization
import org.apache.kyuubi.plugin.spark.authz.security.{DatarkSparkAccessRequest, DatarkSparkAuthentication}
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._

case class RuleAuthorization(spark: SparkSession) extends Authorization(spark) {

    private val LOG = LogFactory.getLog(classOf[RuleAuthorization])

  override def checkPrivileges(spark: SparkSession, plan: LogicalPlan): Unit = {
    val auditHandler = new SparkRangerAuditHandler
    val ugi = getAuthzUgi(spark.sparkContext)
    val (inputs, outputs, opType) = PrivilegesBuilder.build(plan, spark)

    // Use a HashSet to deduplicate the same AccessResource and AccessType, the requests will be all
    // the non-duplicate requests and in the same order as the input requests.
    val requests = new mutable.ArrayBuffer[AccessRequest]()
    val requestsSet = new mutable.HashSet[(AccessResource, AccessType)]()

    def addAccessRequest(objects: Iterable[PrivilegeObject], isInput: Boolean): Unit = {
      objects.foreach { obj =>
        val resource = AccessResource(obj, opType)
        val accessType = ranger.AccessType(obj, opType, isInput)
        if (accessType != AccessType.NONE && !requestsSet.contains((resource, accessType))) {
          requests += AccessRequest(resource, ugi, opType, accessType)
          requestsSet.add((resource, accessType))
        }
      }
    }

    addAccessRequest(inputs, isInput = true)
    addAccessRequest(outputs, isInput = false)

    val requestArrays = requests.map { request =>
      val resource = request.getResource.asInstanceOf[AccessResource]
      resource.objectType match {
        case ObjectType.COLUMN if resource.getColumns.nonEmpty =>
          resource.getColumns.map { col =>
            val cr =
              AccessResource(
                COLUMN,
                resource.getDatabase,
                resource.getTable,
                col,
                Option(resource.getOwnerUser),
                resource.catalog)
            AccessRequest(cr, ugi, opType, request.accessType).asInstanceOf[RangerAccessRequest]
          }
        case _ => Seq(request)
      }
    }.toSeq

    if (authorizeInSingleCall) {
      verify(requestArrays.flatten, auditHandler)
    } else {
      requestArrays.flatten.foreach { req =>
        verify(Seq(req), auditHandler)
      }
    }
  }

  override def checkPrivileges2(spark: SparkSession, plan: LogicalPlan): Unit = {

    val (userName, datarkUrl, appCode, expireTime, auditEnable, datarkQueryType, datarkTaskId, projectCode, throwableException) = getConfig(spark)

//    val auditHandler = new SparkRangerAuditHandler
//    val ugi = getAuthzUgi(spark.sparkContext)
    val (inputs, outputs, opType) = PrivilegesBuilder.build(plan, spark)

    // Use a HashSet to deduplicate the same AccessResource and AccessType, the requests will be all
    // the non-duplicate requests and in the same order as the input requests.
    val requests = new mutable.ArrayBuffer[DatarkSparkAccessRequest]()
    val requestsSet = new mutable.HashSet[(AccessResource, AccessType)]()

    def addAccessRequest(objects: Iterable[PrivilegeObject], isInput: Boolean): Unit = {
      objects.foreach { obj =>
        val resource = AccessResource(obj, opType)
        val accessType = ranger.AccessType(obj, opType, isInput)
        if (accessType != AccessType.NONE && !requestsSet.contains((resource, accessType))) {
          requests += new DatarkSparkAccessRequest(resource, userName, opType.toString, accessType.toString.toLowerCase(Locale.ROOT),
            datarkUrl, appCode, expireTime, auditEnable, datarkQueryType, datarkTaskId, projectCode)
//          requests += AccessRequest(resource, ugi, opType, accessType)
          requestsSet.add((resource, accessType))
        }
      }
    }

    addAccessRequest(inputs, isInput = true)
    addAccessRequest(outputs, isInput = false)

    val requestArrays = requests.map { request =>
      val resource = request.getResource.asInstanceOf[AccessResource]
      resource.objectType match {
        case ObjectType.COLUMN if resource.getColumns.nonEmpty =>
          resource.getColumns.map { col =>
            val cr =
              AccessResource(
                COLUMN,
                resource.getDatabase,
                resource.getTable,
                col,
                Option(resource.getOwnerUser),
                resource.catalog)
//            AccessRequest(cr, ugi, opType, request.accessType).asInstanceOf[RangerAccessRequest]
            val request1 = request.copy()
            request1.setResource(cr)
            request1
          }
        case _ => Seq(request)
      }
    }.toSeq

    requestArrays.flatten.foreach {request =>
      val allowed = DatarkSparkAuthentication.isAccessAllowed(request, true)
      if (!allowed && "true".equalsIgnoreCase(throwableException)) {
          val msg = s"Permission denied: user [$userName] does not" +
                  s" have [${request.getAccessType}] privilege on [${DatarkSparkAuthentication.getAsString(request.getResource)}]"
          LOG.error(
              s"""
                 |+===============================+
                 ||Spark SQL Authorization Failure|
                 ||-------------------------------|
                 ||${msg}
                 ||-------------------------------|
                 ||Spark SQL Authorization Failure|
                 |+===============================+
               """.stripMargin)

          throw new AccessControlException(msg)
      }
    }
  }
}
