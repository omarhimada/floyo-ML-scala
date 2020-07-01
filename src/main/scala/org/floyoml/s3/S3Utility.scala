package org.floyoml.s3

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.iterable.S3Objects
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import org.floyoml._
import org.floyoml.input.{ChurnPrediction, MLTask, Recommendations, Segmentation}
import org.floyoml.shared.{Configuration, Context}

object S3Utility {
  /**
   * Configure AWS S3 integration
   */
  Context.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", Configuration.EnvironmentVariables.awsAccessKey)
  Context.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", Configuration.EnvironmentVariables.awsSecretKey)
  Context.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", Configuration.awsS3Endpoint)

  private val s3Client = initS3Client

  /**
   * Initialize a new AWS S3 client using the AWS Java SDK
   * @return
   */
  private def initS3Client: AmazonS3 = AmazonS3ClientBuilder.standard().withCredentials(s3CredentialsProvider).build()

  /**
   * Retrieve AWS S3 credentials from the environment
   */
  private val s3CredentialsProvider: AWSCredentialsProvider =
    new AWSCredentialsProvider {
      final private val credentials =
        new BasicAWSCredentials(
          Configuration.EnvironmentVariables.awsAccessKey,
          Configuration.EnvironmentVariables.awsSecretKey)

      override
      def getCredentials: AWSCredentials = credentials

      override
      def refresh(): Unit = {
      }
    }

  /**
   * Retrieve the relevant paths to use for streaming the data from S3
   * @param process the type of ML task being executed (e.g.: recommendations, segmentation, etc.)
   * @param isTraining whether or not the data being retrieved is for testing the ML model
   */
  def retrieveS3ObjectPathsForStreaming(process: MLTask, isTraining: Boolean): ListBuffer[String] = {
    val bucket = getProcessBucketFor(process)
    val suffix = getProcessBucketPathSuffix(isTraining)

    val objects = traverseBucket(bucket)

    val paths = ListBuffer.empty[String]

    for (obj <- objects) {
      val path = s"$bucket/$suffix/$obj"
      paths += s"s3://$path"
    }

    paths
  }

  /**
   * Traverse the provided AWS S3 bucket and return a collection of all its files
   * @param inputBucket AWS S3 bucket to traverse
   */
  private def traverseBucket(inputBucket: String): ListBuffer[String] = {
    // objects in S3 are separated by "/"
    val fileSeparator = "/"

    // whether or not the current key is a directory
    def isS3Directory(str: String): Boolean = str.endsWith(fileSeparator)

    val foundObjects = ListBuffer.empty[String]

    for (s3Object <- S3Objects.withPrefix(s3Client, inputBucket, "").asScala) {
      if (!isS3Directory(s3Object.getKey)) {
        foundObjects += s3Object.getKey
      }
    }

    foundObjects
  }

  /**
   * Determine the suffix (directory) to use for the AWS S3 path
   * @param isTraining whether or not the data being retrieved is for testing the ML model
   * @return data-to-train directory, or data-to-process directory
   */
  private def getProcessBucketPathSuffix(isTraining: Boolean): String =
    if (isTraining) Configuration.S3.Common.dataToTrainWithDirectory
    else Configuration.S3.Common.dataToProcessDirectory

  /**
   * Determine the bucket to use for a specific ML task
   * @param process the type of ML task being executed (e.g.: recommendations, segmentation, etc.)
   * @return the AWS S3 bucket to use for the given ML task
   */
  private def getProcessBucketFor(process: MLTask): String =
    process match {
      case Segmentation(_) => Configuration.S3.kMeansDataDirectory
      case Recommendations(_) => throw new NotImplementedError("to-do")
      case ChurnPrediction(_) => throw new NotImplementedError("to-do")
    }
}
