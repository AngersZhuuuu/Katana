package org.apache.spark.sql.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation, SessionCatalog}
import org.apache.spark.sql.internal.SessionState

import scala.collection.mutable

/**
 * @author angers.zhu@gmail.com
 * @date 2019/5/28 11:03
 */
object CatalogSchemaUtil {
  def sliceHiveSchema(withSchema: String, schema: String): String = {
    withSchema.replaceFirst(s"${schema}_", "")
  }

  def getCatalog(hiveCatalogs: mutable.HashMap[String, SessionCatalog],
                 hiveTableRelation: HiveTableRelation)(sparkSession: SparkSession): SessionCatalog = {
    hiveCatalogs.values.find(catalog => {
      catalog.tableExists(hiveTableRelation.tableMeta.identifier)
    }).getOrElse(sparkSession.sessionState.catalog)
  }

  def getCatalog(hiveCatalogs: mutable.HashMap[String, SessionCatalog],
                 tableMeta: CatalogTable)(sparkSession: SparkSession): SessionCatalog = {
    hiveCatalogs.values.find(catalog => {
      catalog.tableExists(tableMeta.identifier)
    }).getOrElse(sparkSession.sessionState.catalog)
  }

  def getSessionStateByTableIdentifier(sessionStates: mutable.HashMap[String, SessionState],
                                       tableIdentifier: TableIdentifier)
                                      (sparkSession: SparkSession, katanaContext: KatanaContext): SessionState = {

    if (tableIdentifier.database.isEmpty) {
      if (katanaContext.getActiveSessionState() == null)
        sparkSession.sessionState
      else
        katanaContext.getActiveSessionState()
    } else {
      getSessionStateByDBName(sessionStates, tableIdentifier.database.get)(sparkSession)
    }
  }

  def getSessionStateByOriginTableIdentifier(sessionStates: mutable.HashMap[String, SessionState],
                                             tableIdentifier: TableIdentifier)
                                            (sparkSession: SparkSession): SessionState = {
    sessionStates.values.find(sessionState => {
      sessionState.catalog.tableExists(tableIdentifier)
    }).getOrElse(sparkSession.sessionState)
  }

  def getSessionStateByDBName(sessionStates: mutable.HashMap[String, SessionState],
                              dbName: String)
                             (sparkSession: SparkSession): SessionState = {
    val token = dbName.split(".")
    if (token.length == 1) {
      sparkSession.sessionState
    } else if (token.length == 2) {
      val catalogName = token(0)
      if (sessionStates.contains(catalogName)) {
        sessionStates(catalogName)
      } else {
        throw new RuntimeException(s"Can't find corresponding hive catalog [${catalogName}]")
      }
    } else {
      throw new RuntimeException(s"Can't parse user passed database name : ${dbName}")
    }
  }

  def getCatalogAndOriginDBName(hiveCatalogs: mutable.HashMap[String, SessionCatalog],
                                dbName: String)(sparkSession: SparkSession): (SessionCatalog, String) = {
    val token = dbName.split(".")
    if (token.length == 1) {
      (sparkSession.sessionState.catalog, dbName)
    } else if (token.length == 2) {
      val catalogName = token(0)
      val originDb = token(1)
      if (hiveCatalogs.contains(catalogName)) {
        (hiveCatalogs(catalogName), originDb)
      } else {
        throw new RuntimeException(s"Can't find corresponding hive catalog [${catalogName}]")
      }
    } else {
      throw new RuntimeException(s"Can't parse user passed database name : ${dbName}")
    }
  }

}
