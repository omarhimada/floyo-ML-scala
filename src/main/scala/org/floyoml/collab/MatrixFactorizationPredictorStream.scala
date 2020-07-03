package org.floyoml.collab

import com.sksamuel.elastic4s.IndexAndType
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.floyoml.elasticsearch.ElasticsearchWriter
import org.floyoml.input.Recommendations
import org.floyoml.output.RatedTransaction
import org.floyoml.s3.S3Utility
import org.floyoml.shared.{Configuration, Context}

import scala.collection.mutable.ListBuffer

object MatrixFactorizationPredictorStream {
  /**
   * Use a persisted Matrix Factorization model to make predictions
   * @param persistedMatrixFactorizationModel an existing, trained Matrix Factorization model
   * @param predictionOutputLocation where to save the predictions that were processed
   * @param writeToElasticsearch whether or not to write the predictions to Elasticsearch
   */
  def run (persistedMatrixFactorizationModel: MatrixFactorizationModel, predictionOutputLocation: String, writeToElasticsearch: Boolean): Unit = {
    // initialize a new StreamingContext to retrieve data from AWS S3
    val streamingContext = new StreamingContext(Context.sparkContext, Seconds(60))

    // get the paths to the objects in S3 that will be used to make predictions with the model
    val objectPaths = S3Utility.retrieveS3ObjectPathsForStreaming(Recommendations(false), isTraining = false)

    // for each relevant object in S3...
    for (objectPath <- objectPaths) {
      val dStream = streamingContext.textFileStream(objectPath)

      val streamOfRatedTransactions: DStream[RatedTransaction] =
        dStream
          .map(_.split(',') match {
            // transform a stream of strings to rated transactions for ALS
            case Array(user, sku, total) => RatedTransaction(user.toInt, sku.toInt, total.toDouble)
          })

      // for each RDD of transactions being streamed from the object in S3...
      streamOfRatedTransactions.foreachRDD { rdd =>
        rdd.foreach { _ =>
          // convert the ratings to an RDD of integer tuples...
          val toPredict = rdd.map {
            case RatedTransaction(user, sku, total) => (user, sku)
          }

          // run the persisted model against the RDD of integer tuples
          val predictions = persistedMatrixFactorizationModel.predict(toPredict)

          if (predictionOutputLocation != null) {
            // save to results to the file, if file name specified
            predictions.saveAsTextFile(predictionOutputLocation)
          }

          if (writeToElasticsearch) {
            // write the predictions to Elasticsearch
            val esWriter = new ElasticsearchWriter[RatedTransaction](
              uri = Configuration.EnvironmentVariables.elasticsearchURI,
              rollingDate = true,
              indexAndType = IndexAndType(Configuration.Elasticsearch.matrixFactorizationTrainingIndex, classOf[RatedTransaction].getSimpleName),
              numberOfBulkDocumentsToWrite = 10,
              initialDocuments = ListBuffer.empty[RatedTransaction]
            )

            // use foreach on the RDD to loop over the predictions
            predictions.foreach { rating =>
              // write each prediction to Elasticsearch
              esWriter.write(Seq(RatedTransaction(rating.user, rating.product, rating.rating)))
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
