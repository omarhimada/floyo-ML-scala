package org.floyoml.kmeans

import java.io.File

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}

import org.floyoml.input.Segmentation
import org.floyoml.s3.S3Utility
import org.floyoml.shared.{Context, Utility}

object KMeansTrainer {
  /**
   * Number of iterations of the K-Means algorithm to execute
   */
  private val _iterations = 20

  /**
   * iterate for value K (number of clusters) beginning at 2
   */
  private val _minNumberOfClusters = 2

  /**
   * iterate for value K (number of clusters) up to 12
   */
  private val _maxNumberOfClusters = 12

  /**
   * Execute K-Means training
   * @param modelLocation location to persist the trained model
   * @return completed K-Means model
   */
  def train(modelLocation: String): KMeansModel = {
    if (new File(modelLocation).exists) Utility.deletePreviousModel(modelLocation)

    // get the paths to the  latest segmentation training data in S3
    val objectPaths = S3Utility.retrieveS3ObjectPathsForStreaming(Segmentation(true), isTraining = true)

    // union our many datasets to a single RDD
    val datasetToTrain = Utility.unionManyDatasets(objectPaths)

    // parse and cache
    val parsedData = datasetToTrain.map(Utility.featurize).cache

    /**
     * use the "Within Set Sum of Squared Errors" evaluation
     * to determine the ideal number of clusters
     */
    var modelWithIdealNumberOfClusters: KMeansModel = null
    var lowestWSSSE: Double = Double.MaxValue
    var idealNumberOfClusters: Int = 2

    for (clusters <- _minNumberOfClusters to _maxNumberOfClusters) {
      // train model
      val model = KMeans.train(parsedData, clusters, _iterations)

      // evaluate "Within Set Sum of Squared Errors"
      val withinSetSumOfSquaredErrors = model.computeCost(parsedData)

      // keep track of the the 'best' model (ideal number of clusters)
      if (withinSetSumOfSquaredErrors < lowestWSSSE) {
        lowestWSSSE = withinSetSumOfSquaredErrors
        modelWithIdealNumberOfClusters = model
        idealNumberOfClusters = clusters
      }
    }

    // persist the trained model
    modelWithIdealNumberOfClusters.save(Context.sparkContext, modelLocation)

    modelWithIdealNumberOfClusters

    //      val example =
    //        trainRdd
    //          .sample(withReplacement = false, 0.1)
    //          .map(s => (s, model.predict(Utility.featurize(s))))
    //          .collect()
    //
    //      println("Prediction examples:")
    //      example.foreach(println)
  }
}
