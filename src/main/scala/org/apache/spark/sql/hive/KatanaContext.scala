package org.apache.spark.sql.hive

import java.util.concurrent._

import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.conf.KatanaConf
import org.apache.spark.sql.internal.StaticSQLConf

import scala.collection.mutable.HashMap
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration.Duration

/**
 * @author angers.zhu@gmail.com
 * @date 2019/5/28 19:27
 */
class KatanaContext(val sparkSession: SparkSession) extends Logging {

  import KatanaContext._

  val sessionId = sparkSession.hashCode()
  private var activeSparkSession:Option[SparkSession] = None
  val sessions = HashMap.empty[String, SparkSession]

  def initial(): Unit = {
    init(sparkSession)
    getOrCrateHiveCatalog()
  }

  def getActiveSession(): Option[SparkSession] = activeSparkSession

  def setActiveSession(sparkSession: SparkSession):Unit = {
    activeSparkSession = Some(sparkSession)
  }

  def getOrCrateHiveCatalog(): Unit = {
    if (sessions.isEmpty) {
      uriMap.keySet.foreach(schema => {
        val url: String = uriMap(schema)
        val warehouse: String = warehouseMap(schema)
        val session = externalSparkSession(schema)
        session.sparkContext.conf.set(HIVE_METASTORE_URIS, url)
        session.sparkContext.conf.set(HIVE_METASTORE_WAREHOUSE, warehouse)
        session.sparkContext.conf.set(WAREHOUSE_PATH, warehouse)
        /**
         * For lazy var, we should initial it before change SparkContext's configuration
         */
        val (newSession, _, _) = {
          val newSession = session.newSession()
          (newSession, newSession.sessionState, newSession.sessionState.catalog.asInstanceOf[HiveSessionCatalog])
        }
        session.sparkContext.conf.remove(HIVE_METASTORE_URIS)
        session.sparkContext.conf.remove(HIVE_METASTORE_WAREHOUSE)
        session.sparkContext.conf.remove(WAREHOUSE_PATH)

        sessions.put(schema, newSession)
      })
    }
  }
}

object KatanaContext extends Logging {

  var INTERNAL_HMS_NAME = ""

  private val INTERNAL_HMS_NAME_DEFAULT = "default"
  private val HIVE_METASTORE_URIS = "hive.metastore.uris"
  private val HIVE_METASTORE_WAREHOUSE = "hive.metastore.warehouse.dir"
  private val HIVE_STAGING_DIR = "hive.exec.stagingdir"
  private val HIVE_SCRATCH_DIR = "hive.exec.scratchdir"
  private val WAREHOUSE_PATH = "spark.sql.warehouse.dir"
  private val SPARK_PREFIX = "spark"

  private var initialPool: Option[ThreadPoolExecutor] = None
  private val OBJECT_LOC = new Object()
  private val catalogToSparkSession = new ConcurrentHashMap[String, SparkSession]()
  private val katanaConfigUrl: HashMap[String, String] = HashMap.empty[String, String]
  private val katanaConfigWarehouse: HashMap[String, String] = HashMap.empty[String, String]
  private val katanaConfigStagingDir: HashMap[String, String] = HashMap.empty[String, String]
  private val katanaConfigScratchDirMap: HashMap[String, String] = HashMap.empty[String, String]

  def uriMap: HashMap[String, String] = katanaConfigUrl

  def warehouseMap: HashMap[String, String] = katanaConfigWarehouse

  def stagingDir(catalog: Option[String]): Option[String] = {
    catalog.map(katanaConfigStagingDir.getOrElse(_, null))
  }

  def scratchDir(catalog: Option[String]): Option[String] = {
    catalog.map(katanaConfigScratchDirMap.getOrElse(_, null))
  }

  def init(sparkSession: SparkSession): Unit = OBJECT_LOC.synchronized {
    if (initialPool.isEmpty) {
      logInfo("Initial External SparkSession")
      initialThreadPool(sparkSession.sparkContext.conf)
      initialExternalSession(sparkSession)
    }
  }

  def externalSparkSession(schema: String): SparkSession = catalogToSparkSession.get(schema)

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
    INTERNAL_HMS_NAME = sparkSession.conf.get(KatanaConf.INTERNAL_HMS_NAME, INTERNAL_HMS_NAME_DEFAULT)
    logInfo(s"Katana internal HMS alias is ${INTERNAL_HMS_NAME}")
    val catalogConf = sparkSession.conf.get(KatanaConf.MULTI_HIVE_INSTANCE)
    catalogConf.split("&&").foreach { instance =>
      val catalog = instance.split("->")(0)
      val url = instance.split("->")(1)
      val warehouse = sparkSession.sparkContext.conf.get(SPARK_PREFIX + "." + HIVE_METASTORE_WAREHOUSE + "." + catalog, null)
      val stagingDir = sparkSession.sparkContext.conf.get(SPARK_PREFIX + "." + HIVE_STAGING_DIR + "." + catalog, null)
      val scratch = sparkSession.sparkContext.conf.get(SPARK_PREFIX + "." + HIVE_SCRATCH_DIR + "." + catalog, null)
      if (warehouse == null) {
        throw new Exception("We can't use a external Hive SessionCatalog without Warehouse Configuration")
      } else {
        val session: SparkSession = initialHiveCatalogAndSharedState(sparkSession.sparkContext, warehouse, url)
        catalogToSparkSession.put(catalog, session)
        katanaConfigUrl.put(catalog, url)
        katanaConfigWarehouse.put(catalog, warehouse)
        katanaConfigStagingDir.put(catalog, stagingDir)
        katanaConfigScratchDirMap.put(catalog, scratch)
      }
    }
  }

  def initialHiveCatalogAndSharedState(sparkContext: SparkContext, warehouse: String, url: String): SparkSession = {
    val sparkSession = Promise[SparkSession]()
    val initialSparkSessionThread = new Runnable {
      override def run(): Unit = {
        sparkSession.trySuccess {
          logInfo("Init Katana SparkSession started.........")
          logInfo(s"uri = $url")
          logInfo(s"warehouse = $warehouse")
          val extensionConf = sparkContext.conf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key)
          sparkContext.conf.set(HIVE_METASTORE_URIS, url)
          sparkContext.conf.set(HIVE_METASTORE_WAREHOUSE, warehouse)
          sparkContext.conf.set(WAREHOUSE_PATH, warehouse)
          sparkContext.conf.remove(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key)

          Hive.closeCurrent()
          logInfo("When init Katana SparkSession, close hive client")
          val newSparkSession = new SparkSession(sparkContext)
          newSparkSession.sharedState.externalCatalog

          newSparkSession.sessionState
          newSparkSession.sessionState.catalog.asInstanceOf[HiveSessionCatalog]
          sparkContext.conf.remove(HIVE_METASTORE_URIS)
          sparkContext.conf.remove(HIVE_METASTORE_WAREHOUSE)
          sparkContext.conf.remove(WAREHOUSE_PATH)
          sparkContext.conf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key, extensionConf)
          logInfo("Init Katana SparkSession finished.........")
          newSparkSession
        }
      }
    }
    initialPool.get.execute(initialSparkSessionThread)
    Await.result(sparkSession.future, Duration(300, TimeUnit.SECONDS))
  }
}
