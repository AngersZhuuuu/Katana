package org.apache.spark.sql.hive.execution.command.desc

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.Histogram
import org.apache.spark.sql.execution.command.{DescribeColumnCommand, RunnableCommand}
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}
import org.apache.spark.sql.types.{MetadataBuilder, StringType}

import scala.collection.mutable.ArrayBuffer

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/29 10:57
  */
case class KatanaDescColumns(delegate: DescribeColumnCommand)
                            (@transient private val katana: KatanaContext) extends RunnableCommand {
  override val output: Seq[Attribute] = {
    Seq(
      AttributeReference("info_name", StringType, nullable = false,
        new MetadataBuilder().putString("comment", "name of the column info").build())(),
      AttributeReference("info_value", StringType, nullable = false,
        new MetadataBuilder().putString("comment", "value of the column info").build())()
    )
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog =
      CatalogSchemaUtil.getCatalog(
        delegate.table.catalog,
        sparkSession,
        katana)

    val resolver = sparkSession.sessionState.conf.resolver

    val relation = sparkSession.table(delegate.table).queryExecution.analyzed

    val colName = UnresolvedAttribute(delegate.colNameParts).name
    val field = {
      relation.resolve(delegate.colNameParts, resolver).getOrElse {
        throw new AnalysisException(s"Column $colName does not exist")
      }
    }
    if (!field.isInstanceOf[Attribute]) {
      // If the field is not an attribute after `resolve`, then it's a nested field.
      throw new AnalysisException(
        s"DESC TABLE COLUMN command does not support nested data types: $colName")
    }

    val catalogTable = catalog.getTempViewOrPermanentTableMetadata(delegate.table)
    val colStats = catalogTable.stats.map(_.colStats).getOrElse(Map.empty)
    val cs = colStats.get(field.name)

    val comment = if (field.metadata.contains("comment")) {
      Option(field.metadata.getString("comment"))
    } else {
      None
    }

    val buffer = ArrayBuffer[Row](
      Row("col_name", field.name),
      Row("data_type", field.dataType.catalogString),
      Row("comment", comment.getOrElse("NULL"))
    )
    if (delegate.isExtended) {
      // Show column stats when EXTENDED or FORMATTED is specified.
      buffer += Row("min", cs.flatMap(_.min.map(_.toString)).getOrElse("NULL"))
      buffer += Row("max", cs.flatMap(_.max.map(_.toString)).getOrElse("NULL"))
      buffer += Row("num_nulls", cs.flatMap(_.nullCount.map(_.toString)).getOrElse("NULL"))
      buffer += Row("distinct_count",
        cs.flatMap(_.distinctCount.map(_.toString)).getOrElse("NULL"))
      buffer += Row("avg_col_len", cs.flatMap(_.avgLen.map(_.toString)).getOrElse("NULL"))
      buffer += Row("max_col_len", cs.flatMap(_.maxLen.map(_.toString)).getOrElse("NULL"))
      val histDesc = for {
        c <- cs
        hist <- c.histogram
      } yield histogramDescription(hist)
      buffer ++= histDesc.getOrElse(Seq(Row("histogram", "NULL")))
    }
    buffer
  }

  private def histogramDescription(histogram: Histogram): Seq[Row] = {
    val header = Row("histogram",
      s"height: ${histogram.height}, num_of_bins: ${histogram.bins.length}")
    val bins = histogram.bins.zipWithIndex.map {
      case (bin, index) =>
        Row(s"bin_$index",
          s"lower_bound: ${bin.lo}, upper_bound: ${bin.hi}, distinct_count: ${bin.ndv}")
    }
    header +: bins
  }
}
