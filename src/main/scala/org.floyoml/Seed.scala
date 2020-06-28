package org.floyoml

import com.beust.jcommander.{JCommander, Parameter}
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.{SparkConf, SparkContext}

object Seed {
  object Arguments {
    @Parameter(names = Array("-nc", "--clusters"))
    var clusters: Int = 3

    @Parameter(names = Array("-po", "--outputPredictions"))
    var outputPredictions: Option[String] = None

    /**
     * if provided will train the data and persist a model
     */
    @Parameter(names = Array("-td", "--dataToTrain"))
    var dataToTrain: Option[String] = None

    /**
     * new models will be persisted here,
     * or an existing model will be loaded from here
     */
    @Parameter(names = Array("-ml", "--modelLocation"))
    var modelLocation: Option[String] = None
  }

  def beginKMeans(c: SparkContext): KMeansModel =
    Arguments.modelLocation match {
      case Some(location) =>
        Arguments.dataToTrain match {
          case Some(data) => KMeansTrainer.train(c, data, Arguments.clusters, location)
          case None => new KMeansModel(c.objectFile[Vector](location).collect())
        }
      case None => throw new IllegalArgumentException("No model location or training data was specified")
    }

  def main(args: Array[String]): Unit = {
    // parse arguments
    JCommander.newBuilder.addObject(Arguments).build().parse(args.toArray: _*)

    val sparkConfig = new SparkConf().setAppName(Shared.sparkAppName)
    val sparkContext = new SparkContext(sparkConfig)

    val clusters: KMeansModel = beginKMeans(sparkContext)

    // todo
    // ElasticsearchWriter
  }
}
