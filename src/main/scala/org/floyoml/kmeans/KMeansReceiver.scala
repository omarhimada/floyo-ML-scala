package org.floyoml.kmeans

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class KMeansReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY)
  with Runnable with Logging {

  @transient
  private var thread: Thread = _

  override def onStart(): Unit = {
    thread = new Thread(this)
    thread.start()
  }

  override def onStop(): Unit = {
    thread.interrupt()
  }

  override def run(): Unit = {
    //receive()
  }

  /*private def receive(): Unit = {
    val webSocket = WebSocket().open(webSocketUrl())
    webSocket.listener(new TextListener {
      override def onMessage(message: String) {
        store(message)
      }
    })
  }*/

}