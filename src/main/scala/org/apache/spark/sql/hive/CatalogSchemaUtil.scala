package org.apache.spark.sql.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.internal.SessionState

import scala.collection.mutable

/**
 * @author angers.zhu@gmail.com
 * @date 2019/5/28 11:03
 */
object CatalogSchemaUtil {

  def getCatalog(catalog: Option[String],
                 hiveCatalogs: mutable.HashMap[String, SessionCatalog],
                 sparkSession: SparkSession,
                 katana: KatanaContext): SessionCatalog = {
    catalog match {
      case None =>
        katana.getActiveSessionState().getOrElse(sparkSession.sessionState).catalog
      case Some(str) =>
        if (str == KatanaContext.INTERNAL_HMS_NAME) {
          sparkSession.sessionState.catalog
        } else if (hiveCatalogs.contains(str)) {
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
        if (str == KatanaContext.INTERNAL_HMS_NAME) {
          sparkSession.sessionState
        } else if (sessionStates.contains(str)) {
          sessionStates(str)
        } else {
          throw new RuntimeException(s"Can't find corresponding hive catalog [${str}]")
        }
    }
  }



  def getCatalogName(catalog: SessionCatalog,
                     hiveCatalogs: mutable.HashMap[String, SessionCatalog]): Option[String] = {
    Option(hiveCatalogs.find(_._2 == catalog).map(_._1).getOrElse(KatanaContext.INTERNAL_HMS_NAME))
  }
}
