package org.apache.spark.sql.hive

/**
  * @author angers.zhu@gmail.com
  * @date 2019/5/30 13:23
  */
object KatanaStatus extends Enumeration {
  val NULL, CONSTRUCTED = Value
  type KatanaStatus = Value
}
