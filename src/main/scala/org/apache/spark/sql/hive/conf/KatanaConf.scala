package org.apache.spark.sql.hive.conf

/**
 * @author angers.zhu@gmail.com
 * @date 2019/5/28 9:59
 */
object KatanaConf {
  val SPARK_KATANA_PREFIX = "spark.sql.katana."
  val INTERNAL_HMS_NAME = SPARK_KATANA_PREFIX + "catalog.internal.name"
  val INTERNAL_HMS_NAME_DEFAULT = "default"
  val MULTI_HIVE_INSTANCE = SPARK_KATANA_PREFIX + "catalog.instances"
  val KATANA_INITIAL_POLL_THREAD = SPARK_KATANA_PREFIX + "initialThread"
  val KATANA_INITIAL_POLL_THREAD_ALIVE_MILLISECONDS = SPARK_KATANA_PREFIX + "initialThread.milliseconds"
  val KATANA_INITIAL_POLL_QUEUE_SIZE = SPARK_KATANA_PREFIX + "pool.queue.size"
}
