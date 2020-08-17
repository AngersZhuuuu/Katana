package org.apache.spark.sql.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.SessionCatalog

/**
 * @author angers.zhu@gmail.com
 * @date 2019/5/28 11:03
 */
object CatalogSchemaUtil {

  def getCatalog(catalog: Option[String],
                 sparkSession: SparkSession,
                 katana: KatanaContext): SessionCatalog = {
    getSession(catalog, sparkSession, katana).sessionState.catalog
  }

  def getSession(catalog: Option[String],
                 sparkSession: SparkSession,
                 katana: KatanaContext): SparkSession = {
    catalog match {
      case None =>
        katana.getActiveSession().getOrElse(sparkSession)
      case Some(str) =>
        if (str == KatanaContext.INTERNAL_HMS_NAME) {
          sparkSession
        } else if (katana.sessions.contains(str)) {
          katana.sessions(str)
        } else {
          throw new RuntimeException(s"Can't find corresponding hive catalog [${str}]")
        }
    }
  }

  def getCatalogName(catalog: SessionCatalog,
                     katana: KatanaContext): Option[String] = {
    Option(
      katana.sessions
        .find(_._2.sessionState.catalog == catalog)
        .map(_._1)
        .getOrElse(KatanaContext.INTERNAL_HMS_NAME))
  }
}
