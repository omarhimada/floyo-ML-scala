package org.floyoml

import com.beust.jcommander.{JCommander, Parameter}

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector

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
     * New models will be persisted here, or an existing model will be loaded from here
     */
    @Parameter(names = Array("-ml", "--modelLocation"))
    var modelLocation: Option[String] = None
  }

  /**
   * Initializes the K-Means clustering process
   * @return completed KMeansModel
   */
  def beginKMeans: KMeansModel =
    Arguments.modelLocation match {
      case Some(location) =>
        if (Arguments.train)
          KMeansTrainer.train(location)
        else
          new KMeansModel(Context.sparkContext.objectFile[Vector](location).collect())
      case None => throw new IllegalArgumentException("No model location or training data was specified")
    }

  /**
   * Execution entry
   * @see Arguments
   */
  def main(args: Array[String]): Unit = {
    // parse arguments
    JCommander.newBuilder.addObject(Arguments).build().parse(args.toArray: _*)

    val persistedKMeansModel: KMeansModel = beginKMeans

    /**
     * K-Means
     */
    KMeansPredictorStream.run(
      persistedKMeansModel,
      // persist predictions locally
      predictionOutputLocation = Configuration.Behaviour.Output.kMeansPredictionsLocalPath,
      // persist predictions in Elasticsearch
      writeToElasticsearch = true)
  }
}
