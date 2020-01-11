package org.apache.spark.sql.hive

import java.util.concurrent._

import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.hive.KatanaContext.{HIVE_METASTORE_URIS, HIVE_METASTORE_WAREHOUSE, SPARK_PREFIX, WAREHOUSE_PATH}
import org.apache.spark.sql.hive.conf.KatanaConf
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/28 19:27
  */
class KatanaContext(val sparkSession: SparkSession) extends Logging {
  val sessionId = sparkSession.hashCode()
  val katanaSessionState: HashMap[String, SessionState] = HashMap.empty[String, SessionState]
  val hiveCatalogs: HashMap[String, SessionCatalog] = HashMap.empty[String, SessionCatalog]
  private var activeSessionState: Option[SessionState] = None

  def initial(): Unit = {
    KatanaContext.init(sparkSession)
    getOrCrateHiveCatalog
  }

  def getActiveSessionState(): SessionState = {
    if (activeSessionState.isEmpty)
      null
    else
      activeSessionState.get
  }

  def setActiveSessionState(sessionState: SessionState): Unit = {
    activeSessionState = Some(sessionState)
  }

  def getOrCrateHiveCatalog() = {
    if (hiveCatalogs.isEmpty) {
      KatanaContext.uriMap.keySet.foreach(schema => {
        val url: String = KatanaContext.uriMap.get(schema).get
        val warehouse: String = KatanaContext.warehouseMap.get(schema).get
        val session = KatanaContext.externalSparkSession(schema)
        session.sparkContext.conf.set(HIVE_METASTORE_URIS, url)
        session.sparkContext.conf.set(HIVE_METASTORE_WAREHOUSE, warehouse)
        session.sparkContext.conf.set(WAREHOUSE_PATH, warehouse)
        /**
          * For lazy var, we should initial it before change SparkContext's configuration
          */
        val (sessionState, sessionCatalog) = {
          val newSession = session.newSession()
          (newSession.sessionState, newSession.sessionState.catalog.asInstanceOf[HiveSessionCatalog])
        }
        session.sparkContext.conf.remove(HIVE_METASTORE_URIS)
        session.sparkContext.conf.remove(HIVE_METASTORE_WAREHOUSE)
        session.sparkContext.conf.remove(WAREHOUSE_PATH)
        hiveCatalogs.put(schema, sessionCatalog)
        katanaSessionState.put(schema, sessionState)
      })
    }
  }
}

object KatanaContext extends Logging {
  private val HIVE_METASTORE_URIS = "hive.metastore.uris"
  private val HIVE_METASTORE_WAREHOUSE = "hive.metastore.warehouse.dir"
  private val WAREHOUSE_PATH = "spark.sql.warehouse.dir"
  private val SPARK_PREFIX = "spark"

  private var initialPool: Option[ThreadPoolExecutor] = None
  private val OBJECT_LOC = new Object()
  private val schemaToSparkSession = new ConcurrentHashMap[String, SparkSession]()
  private val hiveConfigUrl: HashMap[String, String] = HashMap.empty[String, String]
  private val hiveConfigWarehouse: HashMap[String, String] = HashMap.empty[String, String]

  def uriMap: HashMap[String, String] = hiveConfigUrl

  def warehouseMap: HashMap[String, String] = hiveConfigWarehouse

  def init(sparkSession: SparkSession): Unit = OBJECT_LOC.synchronized {
    if (initialPool.isEmpty) {
      logInfo("Initial External SparkSession")
      initialThreadPool(sparkSession.sparkContext.conf)
      initialExternalSession(sparkSession)
    }
  }

  def externalSparkSession(schema: String) = schemaToSparkSession.get(schema)

  def initialThreadPool(conf: SparkConf): Unit = {
    val katanaThreadNum = conf.getInt(KatanaConf.KATANA_INITIAL_POLL_THREAD, 3)
    val katanaThreadAlive = conf.getInt(KatanaConf.KATANA_INITIAL_POLL_THREAD_ALIVE_MILLISECONDS, 300000)
    val queueSize = conf.getInt(KatanaConf.KATANA_INITIAL_POLL_QUEUE_SIZE, 3)
    initialPool = Some(
      new ThreadPoolExecutor(
        katanaThreadNum,
        katanaThreadNum,
        katanaThreadAlive,
        TimeUnit.MICROSECONDS,
        new LinkedBlockingQueue[Runnable](queueSize),
        Executors.defaultThreadFactory))
    initialPool.get.allowCoreThreadTimeOut(false)
  }

  def initialExternalSession(sparkSession: SparkSession): Unit = OBJECT_LOC synchronized {
    val catalogConf = sparkSession.conf.get(KatanaConf.MULTI_HIVE_INSTANCE)
    catalogConf.split("&&").foreach(instance => {
      val schema = instance.split("->")(0)
      val url = instance.split("->")(1)
      val warehouse = sparkSession.sparkContext.conf.get(SPARK_PREFIX + "." + HIVE_METASTORE_WAREHOUSE + "." + schema, null)
      if (warehouse == null)
        throw new Exception("We can't use a external Hive SessionCatalog without Warehouse Configuration")
      else {
        val session: SparkSession = initialHiveCatalogAndSharedState(sparkSession.sparkContext, warehouse, url)
        schemaToSparkSession.put(schema, session)
        hiveConfigUrl.put(schema, url)
        hiveConfigWarehouse.put(schema, warehouse)
      }
    })
  }

  def initialHiveCatalogAndSharedState(sparkContext: SparkContext, warehouse: String, url: String): SparkSession = {
    val sparkSession = Promise[SparkSession]()
    val initialSparkSessionThread = new Runnable {
      override def run(): Unit = {
        sparkSession.trySuccess {
          sparkContext.conf.set(HIVE_METASTORE_URIS, url)
          sparkContext.conf.set(HIVE_METASTORE_WAREHOUSE, warehouse)
          sparkContext.conf.set(WAREHOUSE_PATH, warehouse)

          Hive.closeCurrent()
          val newSparkSession = new SparkSession(sparkContext)
          newSparkSession.sharedState.externalCatalog

          newSparkSession.sessionState
          newSparkSession.sessionState.catalog.asInstanceOf[HiveSessionCatalog]
          sparkContext.conf.remove(HIVE_METASTORE_URIS)
          sparkContext.conf.remove(HIVE_METASTORE_WAREHOUSE)
          sparkContext.conf.remove(WAREHOUSE_PATH)
          newSparkSession
        }
      }
    }
    initialPool.get.execute(initialSparkSessionThread)
    Await.result(sparkSession.future, Duration(100, TimeUnit.SECONDS))
  }
}
