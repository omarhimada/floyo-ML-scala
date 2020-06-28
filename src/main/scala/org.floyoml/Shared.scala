package org.floyoml

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector

object Shared {
  private val _dimensions: Int = 1000
  private val tf = new HashingTF(_dimensions)

  /**
   * app name for Apache Spark
   */
  val sparkAppName = "floyo.scala.Seed"

  /**
   * name of the environment variable that configures the AWS access key
   */
  val environmentVariableAWSAccessKey = "AWS_ACCESS_KEY"

  /**
   * name of the environment variable that configures the AWS secret key
   */
  val environmentVariableAWSSecretKey = "AWS_SECRET_KEY"

  val awsS3Endpoint = "s3.amazonaws.com"

  /**
   * transform (min-hash) string to Vector[Double] for KMeans
   */
  def featurize(s: String): Vector = tf.transform(s.sliding(2).toSeq) // (s.toSeq.sliding(2).map(_.unwrap).toSeq)
}
