package org.floyoml

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
    // todo
    val training = true
    val objectPaths = S3Utility.retrieveS3ObjectPathsForStreaming(Segmentation(training), training)

    for (path <- objectPaths) {
      // todo don't just store the path, get the data and store the data
      store(path)
    }
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