package org.apache.spark.sql.hive

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions, Strategy}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.hive.analyze.{KatanaHiveDDLRule, KatanaHiveRelationRule}
import org.apache.spark.sql.hive.parser.KatanaIdentifierParser
import org.apache.spark.sql.hive.strategy.{KatanaBasicOperators, KatanaHiveStrategies}
import org.apache.spark.util.KeyLock

import scala.collection.mutable

/**
 * @author angers.zhu@gmail.com
 * @date 2019/5/28 9:47
 */
class KatanaExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(sessionExtensions: SparkSessionExtensions): Unit = {
    logInfo("Apply KatanaExtension .....")
    type RuleBuilder = SparkSession => Rule[LogicalPlan]
    type StrategyBuilder = SparkSession => Strategy
    type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface

    val ugi = UserGroupInformation.getCurrentUser
    //    Cn't support Inject Parser
    sessionExtensions.injectParser(KatanaIdentifierParser(KatanaExtension.getOrCreateKatanaContext(_, ugi)))
    //    Analysis UnresolvedRelation Router
    sessionExtensions.injectResolutionRule(KatanaHiveRelationRule(KatanaExtension.getOrCreateKatanaContext(_, ugi)))
    //    DESC/SHOW/CREATE/DROP Schema  Router
    sessionExtensions.injectResolutionRule(KatanaHiveDDLRule(KatanaExtension.getOrCreateKatanaContext(_, ugi)))
    //    DDL[ CREATE TABLE AS SELECT / INSERT / CREATE TABLE / CREATE TABLE LIKE ]  Router
    sessionExtensions.injectPlannerStrategy(KatanaBasicOperators(KatanaExtension.getOrCreateKatanaContext(_, ugi)))
    //    HiveStrategy Router
    sessionExtensions.injectPlannerStrategy(KatanaHiveStrategies(KatanaExtension.getOrCreateKatanaContext(_, ugi)))
  }
}

object KatanaExtension {

  private val user2KatanaContext = mutable.HashMap.empty[String, KatanaContext]
  private val keyLock = new KeyLock[String]

  def getOrCreateKatanaContext(
      sparkSession: SparkSession,
      ugi: UserGroupInformation): KatanaContext = {
    keyLock.withLock(ugi.getShortUserName) {
      if (user2KatanaContext.contains(ugi.getShortUserName)) {
        user2KatanaContext(ugi.getShortUserName)
      } else {
        val katana = new KatanaContext(sparkSession, ugi.getShortUserName)
        katana.initial()
        user2KatanaContext.put(ugi.getShortUserName, katana)
        user2KatanaContext(ugi.getShortUserName)
      }
    }
  }
}

