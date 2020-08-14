package org.apache.spark.sql.hive.conf

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/28 9:59
  */
object KatanaConf {
  val INTERNAL_HMS_NAME = "spark.sql.hive.catalog.internal.name"
  val MULTI_HIVE_INSTANCE = "spark.sql.hive.catalog.instances"
  val KATANA_INITIAL_POLL_THREAD = "spark.sql.hive.katana.initialThread"
  val KATANA_INITIAL_POLL_THREAD_ALIVE_MILLISECONDS = "spark.sql.hive.katana.initialThread.milliseconds"
  val KATANA_INITIAL_POLL_QUEUE_SIZE = "spark.sql.hive.katana.pool.queue.size"
}
