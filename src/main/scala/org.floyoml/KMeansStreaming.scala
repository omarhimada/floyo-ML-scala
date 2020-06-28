package org.floyoml

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import spray.json._
import DefaultJsonProtocol._

object KMeansStreaming {
  def run(sparkContext: SparkContext, clusters: KMeansModel, predictOutput: String) {
    val ssc = new StreamingContext(sparkContext, Seconds(5))
    val dStream = ssc.receiverStream(new KMeansReceiver)

    val stream: DStream[String] =
      dStream
        // filter data from stream and marshall to JSON
        // get only message events
        .filter(_.parseJson.convertTo[Map[String, String]].get("type") match {
          case Some(str) => str == "message"
          case None => false
        })
        // extract message text from the event
        .map(_.parseJson.convertTo[Map[String, String]].get("text") match {
          case Some(str) => str
          case None => ""
        })

    // transform stream data and predict clusters
    val kMeansStream = transformAndPredict(stream, clusters)

    /* print k-mean results as pairs (m, c)
     * where m is the message text and c is the associated cluster */
    kMeansStream.print()

    if (predictOutput != null) {
      kMeansStream.saveAsTextFiles(predictOutput) // save to results to the file, if file name specified
    }

    ssc.start() // run spark streaming application
    ssc.awaitTermination() // wait the end of the application
  }

  /**
   * transform stream of strings to stream of (string, vector) tuples and set this stream as input data for prediction
   */
  def transformAndPredict(dStream: DStream[String], clusters: KMeansModel): DStream[(String, Int)] = {
    dStream
      .map(s => (s, Shared.featurize(s)))
      .map(p => (p._1, clusters.predict(p._2)))
  }
}
