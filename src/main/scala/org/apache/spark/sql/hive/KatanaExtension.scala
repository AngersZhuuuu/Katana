package org.apache.spark.sql.hive

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions, Strategy}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.hive.KatanaStatus._
import org.apache.spark.sql.hive.analyze.{KatanaHiveDDLRule, KatanaHiveRelationRule}
import org.apache.spark.sql.hive.parser.KatanaIdentifierParser
import org.apache.spark.sql.hive.strategy.{KatanaBasicOperators, KatanaHiveStrategies}

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/28 9:47
  */
class KatanaExtension extends (SparkSessionExtensions => Unit) {
  override def apply(sessionExtensions: SparkSessionExtensions): Unit = {
    type RuleBuilder = SparkSession => Rule[LogicalPlan]
    type StrategyBuilder = SparkSession => Strategy
    type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface

    //    Cn't support Inject Parser
    sessionExtensions.injectParser(KatanaIdentifierParser(KatanaExtension.getOrCreateKatanaContext))
    //    Analysis UnresolvedRelation Router
    sessionExtensions.injectResolutionRule(KatanaHiveRelationRule(KatanaExtension.getOrCreateKatanaContext))
    //    DESC/SHOW/CREATE/DROP Schema  Router
    sessionExtensions.injectResolutionRule(KatanaHiveDDLRule(KatanaExtension.getOrCreateKatanaContext))
    //    DDL[ CREATE TABLE AS SELECT / INSERT / CREATE TABLE / CREATE TABLE LIKE ]  Router
    sessionExtensions.injectPlannerStrategy(KatanaBasicOperators(KatanaExtension.getOrCreateKatanaContext))
    //    HiveStrategy Router
    sessionExtensions.injectPlannerStrategy(KatanaHiveStrategies(KatanaExtension.getOrCreateKatanaContext))
  }
}

object KatanaExtension {
  private val katanaContext = new ThreadLocal[KatanaContext]()
  //  Avoid re-use in ThriftServer Scenario
  private val katanaStatus = new ThreadLocal[KatanaStatus.Value]()
  private val CONTEXT_LOCK = new Object()

  def constructControl(status: KatanaStatus.Value): Unit = {
    katanaStatus.set(status)
  }

  def getOrCreateKatanaContext(sparkSession: SparkSession): KatanaContext = CONTEXT_LOCK.synchronized {
    if (katanaStatus.get() == null ||
      (katanaContext.get() != null && katanaStatus.get() != CONSTRUCTED) ||
      (katanaContext.get() != null && katanaContext.get().sessionId != sparkSession.hashCode())) {
      katanaContext.remove()
      val katana = new KatanaContext(sparkSession)
      katana.initial()
      katanaContext.set(katana)
      katanaStatus.set(CONSTRUCTED)
    }
    katanaContext.get
  }
}

