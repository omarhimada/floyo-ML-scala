package org.floyoml.kmeans

import java.io.File
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.floyoml.shared.Utility

object KMeansTrainer {
  private val _iterations = 20

  /**
   * Execute K-Means training
   * @param sparkContext existing spark context
   * @param trainingData training data that was received from S3
   * @param numClusters number of clusters ('K' in K-Means)
   * @param modelLocation location to persist the trained model
   * @return completed K-Means model
   */
  def train(sparkContext: SparkContext, trainingData: String, numClusters: Int, modelLocation: String): KMeansModel = {
    if (new File(modelLocation).exists) deletePreviousModel(modelLocation)

    // resilient distributed dataset (RDD) for training
    val trainRdd = sparkContext.textFile(trainingData)

    // parse and cache
    val parsedData = trainRdd.map(Utility.featurize).cache()

    // train model
    val model = KMeans.train(parsedData, numClusters, _iterations)

    // output model as RDD to modelLocation
    sparkContext.makeRDD(model.clusterCenters, numClusters).saveAsObjectFile(modelLocation)

    val example =
      trainRdd
        .sample(withReplacement = false, 0.1)
        .map(s => (s, model.predict(Utility.featurize(s))))
        .collect()

    println("Prediction examples:")
    example.foreach(println)

    model
  }

  /**
   * Delete the previously persisted ML model from the provided path
   * @param path the path of the ML model to delete
   */
  def deletePreviousModel(path: String): Unit = {
    def getRecursively(f: File): Seq[File] =
      f.listFiles.filter(_.isDirectory).flatMap(getRecursively) ++ f.listFiles

    getRecursively(new File(path)).foreach{f =>
      if (!f.delete())
        throw new RuntimeException("Failed to delete " + f.getAbsolutePath)
    }

    new File(path).delete()
  }
}
