package org.floyoml

import com.beust.jcommander.{JCommander, Parameter}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.floyoml.collab.{MatrixFactorizationPredictorStream, MatrixFactorizationTrainer}
import org.floyoml.kmeans.{KMeansPredictorStream, KMeansTrainer}
import org.floyoml.shared.{Configuration, Context}

object Seed {
  /**
   * Arguments passed to Seed, defining the expected behaviour of the process
   */
  object Arguments {
    @Parameter(names = Array("-po", "--outputPredictions"))
    var outputPredictions: Option[String] = None

    /**
     * Whether or not to attempt training
     */
    @Parameter(names = Array("-t", "--train"))
    var train: Boolean = false

    /**
     * New KMeans models will be persisted here, or an existing model will be loaded from here
     */
    @Parameter(names = Array("-km-ml", "--kMeansModelLocation"))
    var kMeansModelLocation: Option[String] = None

    /**
     * New Matrix Factorization models will be persisted here, or an existing model will be loaded from here
     */
    @Parameter(names = Array("-mf-ml", "--matrixFactorizationModelLocation"))
    var matrixFactorizationModelLocation: Option[String] = None
  }

  /**
   * Initializes the K-Means clustering process, either training a new model
   * or loading an existing model
   * @return completed KMeansModel
   */
  def beginKMeans: KMeansModel =
    Arguments.kMeansModelLocation match {
      case Some(location) =>
        if (Arguments.train)
          KMeansTrainer.train(location)
        else
          KMeansModel.load(Context.sparkContext, location)
      case None => throw new IllegalArgumentException("No K-Means model location or training data was specified")
    }

  /**
   * Initializes the Matrix Factorization recommendations process, either training a new model
   * or loading an existing model
   * @return completed MatrixFactorizationModel
   */
  def beginMatrixFactorization: MatrixFactorizationModel =
    Arguments.matrixFactorizationModelLocation match {
      case Some(location) =>
        if (Arguments.train)
          MatrixFactorizationTrainer.train(location)
        else
          MatrixFactorizationModel.load(Context.sparkContext, location)
      case None => throw new IllegalArgumentException("No Matrix Factorization model location or training data was specified")
    }

  /**
   * Execution entry
   * @see Arguments
   */
  def main(args: Array[String]): Unit = {
    // parse arguments
    JCommander.newBuilder.addObject(Arguments).build().parse(args.toArray: _*)

    /**
     * K-Means
     */
    KMeansPredictorStream.run(
      beginKMeans,
      // persist predictions locally
      predictionOutputLocation = Configuration.Behaviour.Output.kMeansPredictionsLocalPath,
      // persist predictions in Elasticsearch
      writeToElasticsearch = true)

    /**
     * Matrix Factorization
     */
    MatrixFactorizationPredictorStream.run(
      beginMatrixFactorization,
      // persist predictions locally
      predictionOutputLocation = Configuration.Behaviour.Output.matrixFactorizationPredictionsLocalPath,
      // persist predictions in Elasticsearch
      writeToElasticsearch = true)
  }
}
