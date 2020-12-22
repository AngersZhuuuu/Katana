package org.apache.spark.sql.hive.utils

import java.util.concurrent.ThreadFactory

class NamedThreadFactory(val namePrefix: String) extends ThreadFactory {
  override def newThread(r: Runnable): Thread = {
    val newThread = new Thread(r)
    newThread.setName(namePrefix + ": Thread-" + newThread.getId)
    newThread
  }
}

