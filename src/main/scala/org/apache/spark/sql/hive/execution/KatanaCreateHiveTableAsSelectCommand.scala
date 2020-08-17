package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.{AnalysisException, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}

import scala.util.control.NonFatal

/**
 * @author angers.zhu@gmail.com
 * @date 2019/5/29 12:05
 */
case class KatanaCreateHiveTableAsSelectCommand(tableDesc: CatalogTable,
                                                query: LogicalPlan,
                                                outputColumnNames: Seq[String],
                                                mode: SaveMode)
                                               (@transient private val session: SparkSession,
                                                @transient private val katanaContext: KatanaContext)
  extends DataWritingCommand {

  private val tableIdentifier = tableDesc.identifier
  private val catalogName = CatalogSchemaUtil.getCatalogName(session.sessionState.catalog, katanaContext)

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    val catalog = session.sessionState.catalog
    if (catalog.tableExists(tableIdentifier)) {
      assert(mode != SaveMode.Overwrite,
        s"Expect the table $tableIdentifier has been dropped when the save mode is Overwrite")

      if (mode == SaveMode.ErrorIfExists) {
        throw new AnalysisException(s"$tableIdentifier already exists.")
      }
      if (mode == SaveMode.Ignore) {
        // Since the table already exists and the save mode is Ignore, we will just return.
        return Seq.empty
      }

      KatanaInsertIntoHiveTable(
        tableDesc,
        Map.empty,
        query,
        overwrite = false,
        ifPartitionNotExists = false,
        outputColumnNames = outputColumnNames)(catalog, catalogName, session)
        .run(sparkSession, child)
    } else {
      // TODO ideally, we should get the output data ready first and then
      // add the relation into catalog, just in case of failure occurs while data
      // processing.
      assert(tableDesc.schema.isEmpty)
      catalog.createTable(
        tableDesc.copy(schema = outputColumns.toStructType), ignoreIfExists = false)

      try {
        // Read back the metadata of the table which was created just now.
        val createdTableMeta = catalog.getTableMetadata(tableDesc.identifier)
        // For CTAS, there is no static partition values to insert.
        val partition = createdTableMeta.partitionColumnNames.map(_ -> None).toMap
        KatanaInsertIntoHiveTable(
          createdTableMeta,
          partition,
          query,
          overwrite = true,
          ifPartitionNotExists = false,
          outputColumnNames = outputColumnNames)(catalog, catalogName, session)
          .run(sparkSession, child)
      } catch {
        case NonFatal(e) =>
          // drop the created table.
          catalog.dropTable(tableIdentifier, ignoreIfNotExists = true, purge = false)
          throw e
      }
    }

    Seq.empty[Row]
  }

  override def argString: String = {
    s"[Database:${tableDesc.database}}, " +
      s"TableName: ${tableDesc.identifier.table}, " +
      s"InsertIntoHiveTable]"
  }
}