package org.floyoml.kmeans

import java.util.UUID

import com.sksamuel.elastic4s.IndexAndType
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.floyoml.elasticsearch.ElasticsearchWriter
import org.floyoml.input.Transaction
import org.floyoml.output.ClusterPrediction
import org.floyoml.shared.{Configuration, Context}

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

    // streams builder
    val builder = new StreamsBuilder

    /**
     * Stream of transactions from the configured transaction topic
     * (KStream[customerId: Double, transaction: Transaction])
     */
    val transactions: KStream[Double, Transaction] =
      builder.stream[Double, Transaction](Configuration.EnvironmentVariables.kafkaTransactionsTopicName)

    // transform the stream of transactions in preparation for prediction
    val transformed = RFMUtility.transformStream(transactions)

    // make predictions using the persisted model
    val predictions = RFMUtility.predictStream(transformed, persistedKMeansModel)

    if (writeToElasticsearch) {
      val esWriter = new ElasticsearchWriter[ClusterPrediction](
        uri = Configuration.EnvironmentVariables.elasticsearchURI,
        rollingDate = true,
        indexAndType = IndexAndType(Configuration.Elasticsearch.kMeansProcessIndex, classOf[ClusterPrediction].getSimpleName),
        numberOfBulkDocumentsToWrite = 10,
        initialDocuments = ListBuffer.empty[ClusterPrediction]
      )

      // use foreach on the stream
      predictions.foreach((k, v) => {
        // write each prediction to Elasticsearch
        esWriter.write(Seq(ClusterPrediction(UUID.randomUUID.toString, v._1, v._2)))
      })
    }

    // run spark streaming application
    streamingContext.start

    // wait until the end of the application
    streamingContext.awaitTermination
  }
}
