package org.apache.spark.sql.hive.execution.command.show

import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.execution.command.{RunnableCommand, SetDatabaseCommand}
import org.apache.spark.sql.hive.{KatanaContext, KatanaExtension, CatalogSchemaUtil}
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.HashMap

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/28 18:19
  */
case class KatanaSetDatabase(delegate: SetDatabaseCommand,
                             hiveCatalog: HashMap[String, SessionCatalog])
                            (@transient private val sessionState: SessionState,
                             @transient private val katana: KatanaContext) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val (_, db: String) = CatalogSchemaUtil.getCatalogAndOriginDBName(hiveCatalog, delegate.databaseName)(sparkSession)
    sessionState.catalog.setCurrentDatabase(db)
    katana.setActiveSessionState(sessionState)
    Seq.empty[Row]
  }
}
