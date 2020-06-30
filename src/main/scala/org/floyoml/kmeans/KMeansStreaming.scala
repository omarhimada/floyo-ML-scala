package org.floyoml.kmeans

import java.util.UUID

import com.sksamuel.elastic4s.Indexable
import com.sksamuel.elastic4s.IndexAndType
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import spray.json._
import DefaultJsonProtocol._
import org.apache.spark.rdd.RDD
import org.floyoml.Segmentation
import org.floyoml.elasticsearch.ElasticsearchWriter
import org.floyoml.s3.S3Utility
import org.floyoml.shared.{Configuration, Context, Utility}

import scala.collection.mutable.ListBuffer

object KMeansStreaming {
  /**
   * Use a persisted K-Means model to make predictions
   * @param persistedKMeansModel an existing, trained K-Means model
   * @param predictionOutputLocation where to save the predictions that were processed
   * @param writeToElasticsearch whether or not to write the predictions to Elasticsearch
   */
  def run(persistedKMeansModel: KMeansModel, predictionOutputLocation: String, writeToElasticsearch: Boolean) {
    // initialize a new StreamingContext to retrieve data from AWS S3
    val streamingContext = new StreamingContext(Context.sparkContext, Seconds(60))

    // get the paths to the objects in S3 that will be used to make predictions with the model
    val objectPaths = S3Utility.retrieveS3ObjectPathsForStreaming(Segmentation(false), isTraining = false)

    // for each relevant object in S3...
    for (objectPath <- objectPaths) {
      val dStream = streamingContext.textFileStream(objectPath)

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
      val streamOfTuples = transformAndPredict(stream, persistedKMeansModel)

      /* print k-mean results as pairs (m, c)
       * where m is the message text and c is the associated cluster */
      streamOfTuples.print()

      if (predictionOutputLocation != null) {
        // save to results to the file, if file name specified
        streamOfTuples.saveAsTextFiles(predictionOutputLocation)
      }

      if (writeToElasticsearch) {
        // write the predictions to Elasticsearch
        val esWriter = new ElasticsearchWriter[ClusterPrediction](
          uri = Configuration.EnvironmentVariables.elasticsearchURI,
          rollingDate = true,
          indexAndType = IndexAndType(Configuration.Elasticsearch.kMeansTrainingIndex, classOf[ClusterPrediction].getSimpleName),
          numberOfBulkDocumentsToWrite = 10,
          initialDocuments = ListBuffer.empty[ClusterPrediction]
        )

        // use foreach on the RDD to loop over the predictions
        streamOfTuples.foreachRDD { rdd =>
          rdd.foreach { case (message, cluster) =>
            // write each prediction to Elasticsearch
            esWriter.write(Seq(ClusterPrediction(UUID.randomUUID.toString, message, cluster)))
          }
        }
      }
    }

    // run spark streaming application
    streamingContext.start

    // wait the end of the application
    streamingContext.awaitTermination
  }

  /**
   * transform stream of strings to stream of (string, vector) tuples and set this stream as input data for prediction
   */
  def transformAndPredict(dStream: DStream[String], persistedKMeansModel: KMeansModel): DStream[(String, Int)] = {
    dStream
      .map(s => (s, Utility.featurize(s)))
      .map(p => (p._1, persistedKMeansModel.predict(p._2)))
  }
}
