package org.floyoml.collab

import java.io.File

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.floyoml.input.Recommendations
import org.floyoml.s3.S3Utility
import org.floyoml.shared.{Context, Utility}

object MatrixFactorizationTrainer {
  private val _rank = 10
  private val _numIterations = 10
  private val _lambda = 0.01

  /**
   * Execute Matrix Factorization training
   * @param modelLocation location to persist the trained model
   * @return completed Matrix Factorization model
   */
  def train(modelLocation: String): MatrixFactorizationModel = {
    if (new File(modelLocation).exists) Utility.deletePreviousModel(modelLocation)

    // get the paths to the  latest recommendations training data in S3
    val objectPaths = S3Utility.retrieveS3ObjectPathsForStreaming(Recommendations(true), isTraining = true)

    // union our many datasets to a single RDD, while parsing to RatedTransactions
    val rddOfRatings = Utility.unionManyDatasets(objectPaths).map(parseRatingsForALS)

    // split the data 80-20 (80% training, 20% testing)
    val splitData = rddOfRatings.randomSplit(Array(0.2, 0.8))

    val modelForRecommendations = ALS.train(splitData(1), _rank, _numIterations, _lambda)

    val rddToTestWith = splitData(0).map {
      case Rating(subject, activity, freq) =>
        (subject, activity)
    }

    val testPredictions = modelForRecommendations.predict(rddToTestWith)

    // todo: write test predictions to Elasticsearch

    // persist the trained model
    modelForRecommendations.save(Context.sparkContext, modelLocation)

    modelForRecommendations
  }

  /**
   * Parse an input string as a Rating in order to train the Matrix Factorization model
   * @param str input string from the downloaded S3 data file
   * @return Rating
   */
  private def parseRatingsForALS(str: String): Rating = {
    val fields = str.split("::")
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
  }
}
