package org.apache.kyuubia.plugin.spark.authz.rule.permanentview

import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

case class PermanentViewMarker(child: LogicalPlan, catalogTable: CatalogTable)
        extends LeafNode with MultiInstanceRelation {

    private val PVM_NEW_INSTANCE_TAG = TreeNodeTag[Unit]("__PVM_NEW_INSTANCE_TAG")

    override def output: Seq[Attribute] = child.output

    override def argString(maxFields: Int): String = ""

    override def innerChildren: Seq[QueryPlan[_]] = child :: Nil

    override def computeStats(): Statistics = child.stats

    override def newInstance(): LogicalPlan = {
        val projectList = child.output.map { case attr =>
            Alias(Cast(attr, attr.dataType), attr.name)(explicitMetadata = Some(attr.metadata))
        }
        val newProj = Project(projectList, child)
        newProj.setTagValue(PVM_NEW_INSTANCE_TAG, ())

        this.copy(child = newProj, catalogTable = catalogTable)
    }

    override def doCanonicalize(): LogicalPlan = {
        child match {
            case p@Project(_, view: View) if p.getTagValue(PVM_NEW_INSTANCE_TAG).contains(true) =>
                view.canonicalized
            case _ =>
                child.canonicalized
        }
    }
}
