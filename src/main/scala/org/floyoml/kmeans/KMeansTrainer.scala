package org.floyoml.kmeans

import org.floyoml.Shared

import java.io.File
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}

object KMeansTrainer {
  private val _iterations = 20

  def train(sparkContext: SparkContext, trainData: String, numClusters: Int, modelLocation: String): KMeansModel = {
    if (new File(modelLocation).exists) deletePreviousModel(modelLocation)

    // resilient distributed dataset (RDD) for training
    val trainRdd = sparkContext.textFile(trainData)

    // parse and cache
    val parsedData = trainRdd.map(Shared.featurize).cache()

    // train model
    val model = KMeans.train(parsedData, numClusters, _iterations)

    // output model as RDD to modelLocation
    sparkContext.makeRDD(model.clusterCenters, numClusters).saveAsObjectFile(modelLocation)

    val example =
      trainRdd
        .sample(withReplacement = false, 0.1)
        .map(s => (s, model.predict(Shared.featurize(s))))
        .collect()

    println("Prediction examples:")
    example.foreach(println)

    model
  }

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
