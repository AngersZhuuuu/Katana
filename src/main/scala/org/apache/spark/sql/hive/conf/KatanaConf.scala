package org.apache.spark.sql.hive.conf

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/28 9:59
  */
object KatanaConf {
  val INTERNAL_HMS_NAME = "spark.sql.katana.catalog.internal.name"
  val MULTI_HIVE_INSTANCE = "spark.sql.katana.catalog.instances"
  val KATANA_INITIAL_POLL_THREAD = "spark.sql.katana.initialThread"
  val KATANA_INITIAL_POLL_THREAD_ALIVE_MILLISECONDS = "spark.sql.katana.initialThread.milliseconds"
  val KATANA_INITIAL_POLL_QUEUE_SIZE = "spark.sql.katana.pool.queue.size"
}
