package org.apache.spark.sql.hive

import java.security.PrivilegedExceptionAction
import java.util.concurrent._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.shims.Utils
import org.apache.hadoop.security.UserGroupInformation
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
class KatanaContext(
    val sparkSession: SparkSession,
    val user: String)
  extends Logging {

  import KatanaContext._

  private val OBJECT_LOC = new Object()
  var INTERNAL_HMS_NAME = ""
  val sessionId = sparkSession.hashCode()
  val ugi = if (UserGroupInformation.isSecurityEnabled) {
    UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser)
  } else {
    UserGroupInformation.createRemoteUser(user)
  }

  private var initialPool: Option[ThreadPoolExecutor] = None
  private var activeSparkSession: Option[SparkSession] = None
  private val catalog2SparkSession = HashMap.empty[String, SparkSession]
  private val catalog2Uris: HashMap[String, String] = HashMap.empty[String, String]
  private val catalog2Warehouses: HashMap[String, String] = HashMap.empty[String, String]
  private val catalog2StagingDirs: HashMap[String, String] = HashMap.empty[String, String]
  private val catalog2ScratchDirs: HashMap[String, String] = HashMap.empty[String, String]

  def stagingDir(catalog: Option[String]): Option[String] = {
    catalog.flatMap(catalog2StagingDirs.get)
  }

  def scratchDir(catalog: Option[String]): Option[String] = {
    catalog.flatMap(catalog2ScratchDirs.get)
  }

  def sessions: HashMap[String, SparkSession] = {
    catalog2SparkSession
  }

  def initial(): Unit = {
    if (initialPool.isEmpty) {
      logInfo("Initial External SparkSession")
      initialThreadPool(sparkSession.sparkContext.conf)
      initialExternalSession(sparkSession, ugi)
    }
  }

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

  def initialExternalSession(
      sparkSession: SparkSession,
      ugi: UserGroupInformation): Unit = OBJECT_LOC synchronized {
    INTERNAL_HMS_NAME = sparkSession.sparkContext.conf.get(KatanaConf.INTERNAL_HMS_NAME, INTERNAL_HMS_NAME_DEFAULT)
    logInfo(s"Katana internal HMS alias is ${INTERNAL_HMS_NAME}")
    val catalogConf = sparkSession.sparkContext.conf.get(KatanaConf.MULTI_HIVE_INSTANCE)
    catalogConf.split("&&").foreach { instance =>
      val catalog = instance.split("->")(0)
      val url = instance.split("->")(1)
      val warehouse = sparkSession.sparkContext.conf.get(SPARK_PREFIX + "." + HIVE_METASTORE_WAREHOUSE + "." + catalog, null)
      val stagingDir = sparkSession.sparkContext.conf.get(SPARK_PREFIX + "." + HIVE_STAGING_DIR + "." + catalog, null)
      val scratch = sparkSession.sparkContext.conf.get(SPARK_PREFIX + "." + HIVE_SCRATCH_DIR + "." + catalog, null)
      if (warehouse == null) {
        throw new Exception("We can't use a external Hive SessionCatalog without Warehouse Configuration")
      } else {
        val session = initialHiveCatalogAndSharedState(sparkSession.sparkContext, catalog, warehouse, url, ugi)
        catalog2SparkSession.put(catalog, session)
        catalog2Uris.put(catalog, url)
        catalog2Warehouses.put(catalog, warehouse)
        catalog2StagingDirs.put(catalog, stagingDir)
        catalog2ScratchDirs.put(catalog, scratch)
      }
    }
  }

  def initialHiveCatalogAndSharedState(
      sparkContext: SparkContext,
      catalog: String,
      warehouse: String,
      url: String,
      ugi: UserGroupInformation): SparkSession = {
    val sparkSession = Promise[SparkSession]()
    val initialSparkSessionThread = new Runnable {
      override def run(): Unit = {
        sparkSession.trySuccess {
          logInfo("Init Katana SparkSession started.........")
          val extensionConf = sparkContext.conf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key)
          sparkContext.conf.set(HIVE_METASTORE_URIS, url)
          sparkContext.conf.set(HIVE_METASTORE_WAREHOUSE, warehouse)
          sparkContext.conf.set(WAREHOUSE_PATH, warehouse)
          sparkContext.conf.remove(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key)

          // Hive connection
          val tokenSignature = sparkContext.conf.getOption("hive.metastore.token.signature")

          val katanaTokenSignature = s"${catalog}_HiveTokenSignature"
          sparkContext.conf.set("hive.metastore.token.signature", katanaTokenSignature)
          sparkContext.hadoopConfiguration.set("hive.metastore.token.signature", katanaTokenSignature)

          val hiveConf = new HiveConf()
          hiveConf.set("hive.metastore.token.signature", "IllegalSignature")
          hiveConf.set(HIVE_METASTORE_URIS, url)
          hiveConf.set(HIVE_METASTORE_WAREHOUSE, warehouse)
          val token = Hive.get(hiveConf).getDelegationToken(user, user)
          Utils.setTokenStr(ugi, token, katanaTokenSignature)

          logInfo(s"Init katana spark session with ugi = ${ugi}")
          val newSparkSession = ugi.doAs(new PrivilegedExceptionAction[SparkSession] {
            override def run(): SparkSession = {
              Hive.closeCurrent()
              SessionState.detachSession()
              val nss = new SparkSession(sparkContext)
              nss.sessionState.catalog.asInstanceOf[HiveSessionCatalog]
              nss.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client
              nss
            }
          })

          sparkContext.conf.remove(HIVE_METASTORE_URIS)
          sparkContext.conf.remove(HIVE_METASTORE_WAREHOUSE)
          sparkContext.conf.remove(WAREHOUSE_PATH)
          sparkContext.conf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key, extensionConf)

          tokenSignature.map { t =>
            sparkContext.conf.set("hive.metastore.token.signature", t)
            sparkContext.hadoopConfiguration.set("hive.metastore.token.signature", t)
          }
          logInfo("Init Katana SparkSession finished.........")
          newSparkSession
        }
      }
    }
    initialPool.get.execute(initialSparkSessionThread)
    Await.result(sparkSession.future, Duration(300, TimeUnit.SECONDS))
  }

  def getActiveSession(): Option[SparkSession] = activeSparkSession

  def setActiveSession(sparkSession: SparkSession): Unit = {
    activeSparkSession = Some(sparkSession)
  }
}

object KatanaContext extends Logging {
  private val INTERNAL_HMS_NAME_DEFAULT = "default"
  private val HIVE_METASTORE_URIS = "hive.metastore.uris"
  private val HIVE_METASTORE_WAREHOUSE = "hive.metastore.warehouse.dir"
  private val HIVE_STAGING_DIR = "hive.exec.stagingdir"
  private val HIVE_SCRATCH_DIR = "hive.exec.scratchdir"
  private val WAREHOUSE_PATH = "spark.sql.warehouse.dir"
  private val SPARK_PREFIX = "spark"
}
