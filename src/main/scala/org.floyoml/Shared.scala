package org.floyoml

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector

/**
 * Configuration and utilities shared across objects
 */
object Shared {
  object Configuration {
    /**
     * app name for Apache Spark
     */
    val sparkAppName = "floyo.scala.Seed"

    val awsS3Endpoint = "s3.amazonaws.com"

    object EnvironmentVariables {
      /**
       * name of the environment variable that configures the AWS access key
       * (you don't need change this - this is just the name of the variable)
       */
      val envAWSAccessKey = "AWS_ACCESS_KEY"

      /**
       * @return name of the AWS access key to use, as configured in the environment variables
       */
      def awsAccessKey: String = sys.env(envAWSAccessKey)

      /**
       * name of the environment variable that configures the AWS secret key
       * (you don't need change this - this is just the name of the variable)
       */
      val envAWSSecretKey = "AWS_SECRET_KEY"

      /**
       * @return name of the AWS secret key to use, as configured in the environment variables
       */
      def awsSecretKey: String = sys.env(envAWSSecretKey)

      /**
       * name of the environment variable that configures the AWS S3 bucket to use
       * (you don't need change this - this is just the name of the variable)
       */
      val envAWSS3BucketName = "AWS_BUCKET_NAME"

      /**
       * @return name of the AWS S3 bucket to use, as configured in the environment variables
       */
      def awsS3BucketName: String = sys.env(envAWSSecretKey)
    }

    /**
     * constants
     */
    object S3 {
      object Common {
        val dataToProcessDirectory = "Process"
        val dataToTrainWithDirectory = "Training"
      }

      val kMeansDataDirectory = "KMeans"
    }
  }

  object Utility {
    private val _dimensions: Int = 1000
    private val tf = new HashingTF(_dimensions)

    /**
     * transform (min-hash) string to Vector[Double] for KMeans
     */
    def featurize(s: String): Vector = tf.transform(s.sliding(2).toSeq) // (s.toSeq.sliding(2).map(_.unwrap).toSeq)
  }
}
