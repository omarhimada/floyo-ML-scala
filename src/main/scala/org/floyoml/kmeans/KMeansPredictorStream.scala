package org.floyoml.kmeans

import java.util.UUID

import com.sksamuel.elastic4s.IndexAndType
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.floyoml.elasticsearch.ElasticsearchWriter
import org.floyoml.input.{Segmentation, Transaction}
import org.floyoml.output.ClusterPrediction
import org.floyoml.s3.S3Utility
import org.floyoml.shared.{Configuration, Context}
import org.joda.time.DateTime
import spray.json._

import scala.collection.mutable.ListBuffer

object KMeansPredictorStream {

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

      val stream: DStream[(Int, DateTime, Double, Double)] =
        dStream
          .map(_.parseJson.convertTo[Transaction])
          // (customerId, date, recency, monetary value)
          .map(t => (t.customerId, t.date, t.unitRecency, t.unitMonetary))

      stream.foreachRDD { rdd =>
        // group by customerId and transform to (customerId, R, F, M)
        val grouped = RFMUtility.groupRddByCustomerIdAndTransform(rdd)

        // remove top and bottom outliers for RFM
        val filtered = RFMUtility.filterGroupedForRFM(grouped)

        // make predictions using the persisted model
        val predictions = RFMUtility.transformRDDAndPredict(filtered, persistedKMeansModel)

        if (writeToElasticsearch) {
          // write the predictions to Elasticsearch
          val esWriter = new ElasticsearchWriter[ClusterPrediction](
            uri = Configuration.EnvironmentVariables.elasticsearchURI,
            rollingDate = true,
            indexAndType = IndexAndType(Configuration.Elasticsearch.kMeansProcessIndex, classOf[ClusterPrediction].getSimpleName),
            numberOfBulkDocumentsToWrite = 10,
            initialDocuments = ListBuffer.empty[ClusterPrediction]
          )

          // use foreach on the RDD to loop over the predictions
          predictions.foreachPartitionAsync { iter =>
            iter.foreach { case (customerId, cluster) =>
              // write each prediction to Elasticsearch
              esWriter.write(Seq(ClusterPrediction(UUID.randomUUID.toString, customerId, cluster)))
            }
          }
        }
      }
    }

    // run spark streaming application
    streamingContext.start

    // wait until the end of the application
    streamingContext.awaitTermination
  }
}
