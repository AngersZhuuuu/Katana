package org.apache.spark.sql.hive.execution

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.hive.KatanaContext

case class KatanaInsertIntoDir(
    isLocal: Boolean,
    storage: CatalogStorageFormat,
    query: LogicalPlan,
    overwrite: Boolean,
    outputColumnNames: Seq[String])
    (@transient private val katana: KatanaContext)
  extends KatanaSaveAsHiveFile {

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    assert(storage.locationUri.nonEmpty)
    SchemaUtils.checkColumnNameDuplication(
      outputColumnNames,
      s"when inserting into ${storage.locationUri.get}",
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    val hiveTable = HiveClientImpl.toHiveTable(CatalogTable(
      identifier = TableIdentifier(storage.locationUri.get.toString, Some("default")),
      tableType = org.apache.spark.sql.catalyst.catalog.CatalogTableType.VIEW,
      storage = storage,
      schema = outputColumns.toStructType
    ))
    hiveTable.getMetadata.put(serdeConstants.SERIALIZATION_LIB,
      storage.serde.getOrElse(classOf[LazySimpleSerDe].getName))

    val tableDesc = new TableDesc(
      hiveTable.getInputFormatClass,
      hiveTable.getOutputFormatClass,
      hiveTable.getMetadata
    )

    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    val jobConf = new JobConf(hadoopConf)

    val targetPath = new Path(storage.locationUri.get)
    val qualifiedPath = FileUtils.makeQualified(targetPath, hadoopConf)
    val (writeToPath: Path, fs: FileSystem) =
      if (isLocal) {
        val localFileSystem = FileSystem.getLocal(jobConf)
        (localFileSystem.makeQualified(targetPath), localFileSystem)
      } else {
        val dfs = qualifiedPath.getFileSystem(hadoopConf)
        (qualifiedPath, dfs)
      }
    if (!fs.exists(writeToPath)) {
      fs.mkdirs(writeToPath)
    }

    // The temporary path must be a HDFS path, not a local path.
    val tmpPath = getExternalTmpPath(sparkSession, hadoopConf, qualifiedPath)(katana)
    val fileSinkConf = new org.apache.spark.sql.hive.HiveShim.ShimFileSinkDesc(
      tmpPath.toString, tableDesc, false)
    logInfo(s"Insert into tmp path ${tmpPath.toString}")
    try {
      saveAsHiveFile(
        sparkSession = sparkSession,
        plan = child,
        hadoopConf = hadoopConf,
        fileSinkConf = fileSinkConf,
        outputLocation = tmpPath.toString)

      logInfo(s"Delete content under path ${writeToPath} before write")
      if (overwrite && fs.exists(writeToPath)) {
        fs.listStatus(writeToPath).foreach { existFile =>
          if (Option(existFile.getPath) != createdTempDir) {
            logInfo(s"Delete content ${existFile} under path ${writeToPath} before write")
            fs.delete(existFile.getPath, true)
          }
        }
      }

      val dfs = tmpPath.getFileSystem(hadoopConf)
      dfs.listStatus(tmpPath).foreach {
        tmpFile =>
          logInfo(s"Rename ${tmpFile.getPath} to ${writeToPath}")
          if (isLocal) {
            dfs.copyToLocalFile(tmpFile.getPath, writeToPath)
          } else {
            dfs.rename(tmpFile.getPath, writeToPath)
          }
      }
    } catch {
      case e: Throwable =>
        throw new SparkException(
          "Failed inserting overwrite directory " + storage.locationUri.get, e)
    } finally {
      deleteExternalTmpPath(hadoopConf)
    }

    Seq.empty[Row]
  }
}
