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

  def getCatalog(catalog: Option[String],
                 hiveCatalogs: mutable.HashMap[String, SessionCatalog],
                 sparkSession: SparkSession,
                 katana: KatanaContext): SessionCatalog = {
    catalog match {
      case None =>
        katana.getActiveSessionState().getOrElse(sparkSession.sessionState).catalog
      case Some(str) =>
        if (hiveCatalogs.contains(str)) {
          hiveCatalogs(str)
        } else {
          throw new RuntimeException(s"Can't find corresponding hive catalog [${str}]")
        }
    }
  }

  def getSessionState(catalog: Option[String],
                      sessionStates: mutable.HashMap[String, SessionState],
                      sparkSession: SparkSession,
                      katana: KatanaContext): SessionState = {
    catalog match {
      case None =>
          katana.getActiveSessionState.getOrElse(sparkSession.sessionState)
      case Some(str) =>
        if (sessionStates.contains(str)) {
          sessionStates(str)
        } else {
          throw new RuntimeException(s"Can't find corresponding hive catalog [${str}]")
        }
    }
  }

  def getCatalogName(catalog: SessionCatalog,
                     hiveCatalogs: mutable.HashMap[String, SessionCatalog]): Option[String] = {
    hiveCatalogs.find(_._2 == catalog).map(_._1)
  }
}
