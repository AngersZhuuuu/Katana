package org.apache.spark.sql.hive.execution.command.show

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.{RunnableCommand, SetDatabaseCommand}
import org.apache.spark.sql.hive.{CatalogSchemaUtil, KatanaContext}

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/28 18:19
  */
case class KatanaSetDatabase(delegate: SetDatabaseCommand)
                            (@transient private val katana: KatanaContext)
  extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val session = CatalogSchemaUtil.getSession(delegate.catalog, sparkSession, katana)
    session.sessionState.catalog.setCurrentDatabase(delegate.databaseName)
    katana.setActiveSession(session)
    Seq.empty[Row]
  }
}
