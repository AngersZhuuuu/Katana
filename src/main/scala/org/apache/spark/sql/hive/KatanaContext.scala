package org.apache.spark.sql.hive

import java.security.PrivilegedExceptionAction
import java.util.concurrent._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars._
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.shims.Utils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.conf.KatanaConf
import org.apache.spark.sql.hive.HiveUtils._
import org.apache.spark.sql.hive.utils.NamedThreadFactory
import org.apache.spark.sql.internal.StaticSQLConf._

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
  var INTERNAL_HMS_NAME = sparkSession.sparkContext.conf.get(KatanaConf.INTERNAL_HMS_NAME, KatanaConf.INTERNAL_HMS_NAME_DEFAULT)

  val sessionId = sparkSession.hashCode()
  val ugi = if (UserGroupInformation.isSecurityEnabled) {
    UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser)
  } else {
    UserGroupInformation.createRemoteUser(user)
  }

  val conf = sparkSession.sparkContext.conf
  val sparkContext = sparkSession.sparkContext

  private var initialPool: Option[ThreadPoolExecutor] = None
  private var activeSparkSession: Option[SparkSession] = None
  private val catalog2SparkSession = HashMap.empty[String, SparkSession]
  private val catalog2StagingDirs = HashMap.empty[String, String]
  private val catalog2ScratchDirs = HashMap.empty[String, String]

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
      initialThreadPool
      initialExternalSession
    }
  }

  def initialThreadPool: Unit = {
    val katanaThreadNum = conf.getInt(KatanaConf.KATANA_INITIAL_POLL_THREAD, 5)
    val katanaThreadAlive = conf.getInt(KatanaConf.KATANA_INITIAL_POLL_THREAD_ALIVE_MILLISECONDS, 300000)
    val queueSize = conf.getInt(KatanaConf.KATANA_INITIAL_POLL_QUEUE_SIZE, 3)
    initialPool = Some(
      new ThreadPoolExecutor(
        katanaThreadNum,
        katanaThreadNum,
        katanaThreadAlive,
        TimeUnit.MICROSECONDS,
        new LinkedBlockingQueue[Runnable](queueSize),
        new NamedThreadFactory("Katana-Init-ThreadPool")))
    initialPool.get.allowCoreThreadTimeOut(false)
  }

  def initialExternalSession: Unit = OBJECT_LOC synchronized {
    conf.get(KatanaConf.MULTI_HIVE_INSTANCE)
      .split(",").map(_.trim).foreach { catalog =>
      val resetValues = NECESSARY_CONF_COLLECTIONS.map { key =>
        key -> conf.getOption(KatanaConf.SPARK_KATANA_PREFIX + key + "." + catalog)
      }.toMap
      if (resetValues(METASTOREWAREHOUSE.varname).isEmpty) {
        throw new Exception("We can't use a external Hive SessionCatalog without Warehouse Configuration")
      } else {
        val session = initialHiveCatalogAndSharedState(catalog, resetValues)
        catalog2SparkSession.put(catalog, session)
        catalog2StagingDirs.put(catalog, resetValues(STAGINGDIR.varname).orNull)
        catalog2ScratchDirs.put(catalog, resetValues(SCRATCHDIR.varname).orNull)
      }
    }
  }

  def initialHiveCatalogAndSharedState(
      catalog: String,
      resetConfValues: Map[String, Option[String]]): SparkSession = {
    val promisedSparkSession = Promise[SparkSession]()
    val initialSparkSessionThread = new Runnable {
      override def run(): Unit = {
        promisedSparkSession.trySuccess {
          logInfo("Init Katana SparkSession started.........")
          val previousValues = NECESSARY_CONF_COLLECTIONS.map(conf.getOption)
          sparkContext.conf.remove(SPARK_SESSION_EXTENSIONS.key)
          resetConfValues(METASTOREWAREHOUSE.varname).foreach(sparkContext.conf.set(WAREHOUSE_PATH, _))
          resetConfValues.foreach { case (key, value) => value.foreach(sparkContext.conf.set(key, _)) }

          // Hive connection
          val tokenSignature = sparkContext.conf.getOption("hive.metastore.token.signature")
          val katanaTokenSignature = s"${catalog}_HiveTokenSignature"
          conf.set("hive.metastore.token.signature", katanaTokenSignature)
          sparkContext.hadoopConfiguration.set("hive.metastore.token.signature", katanaTokenSignature)
          val hiveConf = new HiveConf()
          hiveConf.set("hive.metastore.token.signature", "IllegalSignature")
          resetConfValues.foreach { case (key, value) => value.foreach(hiveConf.set(key, _)) }
          val token = Hive.get(hiveConf).getDelegationToken(user, user)
          Utils.setTokenStr(ugi, token, katanaTokenSignature)

          logInfo(s"Init katana spark session with ugi = ${ugi}")
          val newSparkSession: SparkSession = ugi.doAs(new PrivilegedExceptionAction[SparkSession] {
            override def run(): SparkSession = {
              Hive.closeCurrent()
              SessionState.detachSession()
              val nss = new SparkSession(sparkContext)
              logInfo("Init Katana SparkSession finished...")
              nss.sessionState.catalog.asInstanceOf[HiveSessionCatalog]
              logInfo("Init HiveSession Catalog")
              nss.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client
              nss
            }
          })

          // reset previous conf value
          NECESSARY_CONF_COLLECTIONS.zip(previousValues).foreach { case (key, value) => value.foreach(conf.set(key, _)) }
          tokenSignature.foreach { t =>
            sparkContext.conf.set("hive.metastore.token.signature", t)
            sparkContext.hadoopConfiguration.set("hive.metastore.token.signature", t)
          }
          logInfo("Init Katana SparkSession finished.........")
          newSparkSession
        }
      }
    }
    initialPool.get.execute(initialSparkSessionThread)
    Await.result(promisedSparkSession.future, Duration(300, TimeUnit.SECONDS))
  }

  def getActiveSession(): Option[SparkSession] = activeSparkSession

  def setActiveSession(sparkSession: SparkSession): Unit = {
    activeSparkSession = Some(sparkSession)
  }
}

object KatanaContext extends Logging {

  val NECESSARY_CONF_COLLECTIONS =
    Seq(METASTOREURIS.varname,
      METASTOREWAREHOUSE.varname,
      STAGINGDIR.varname,
      SCRATCHDIR.varname,
      HIVE_METASTORE_JARS.key,
      HIVE_METASTORE_VERSION.key,
      SPARK_SESSION_EXTENSIONS.key)
}
