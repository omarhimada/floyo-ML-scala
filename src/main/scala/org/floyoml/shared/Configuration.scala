package org.floyoml.shared

import java.util.Properties

import org.apache.kafka.streams.StreamsConfig

object Configuration {
  /**
   * Constants related to Kafka
   */
  object Kafka {
    /**
     * Name to use for the Kafka property 'APPLICATION_ID_CONFIG'
     */
    private val transactionsStreamApplicationId = "floyoml-stream-transactions"

    /**
     * URI(s) to use for the Kafka property 'BOOTSTRAP_SERVERS_CONFIG'
     */
    private val bootstrapServers = "localhost:9092"

    /**
     * Kafka properties
     */
    val properties: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, transactionsStreamApplicationId)
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      p
    }
  }
  
  /**
   * App name for Apache Spark
   */
  val sparkAppName = "floyo.scala.Seed"
  val awsS3Endpoint = "s3.amazonaws.com"

  object EnvironmentVariables {
    /**
     * Name of the environment variable that configures the AWS access key
     * (you don't need to change this - this is just the name of the variable)
     */
    private val envKafkaTransactionsTopicName = "TransactionsTopic"

    /**
     * @return name of the Kafka topic to stream transactions from for predictions
     */
    def kafkaTransactionsTopicName: String = sys.env(envKafkaTransactionsTopicName)
    
    /**
     * Name of the environment variable that configures the AWS access key
     * (you don't need to change this - this is just the name of the variable)
     */
    private val envAWSAccessKey = "FLOYOML_AWS_ACCESS_KEY"

    /**
     * @return name of the AWS access key to use, as configured in the environment variables
     */
    def awsAccessKey: String = sys.env(envAWSAccessKey)

    /**
     * Name of the environment variable that configures the AWS secret key
     * (you don't need to change this - this is just the name of the variable)
     */
    private val envAWSSecretKey = "FLOYOML_AWS_SECRET_KEY"

    /**
     * @return name of the AWS secret key to use, as configured in the environment variables
     */
    def awsSecretKey: String = sys.env(envAWSSecretKey)

    /**
     * Name of the environment variable that configures the AWS S3 bucket to use
     * (you don't need to change this - this is just the name of the variable)
     */
    private val envAWSS3BucketName = "FLOYOML_AWS_BUCKET_NAME"

    /**
     * @return name of the AWS S3 bucket to use, as configured in the environment variables
     */
    def awsS3BucketName: String = sys.env(envAWSS3BucketName)

    /**
     * Name of the environment variable that configures the client ID
     * (you don't need to change this - this is just the name of the variable)
     */
    private val envClientId = "FLOYOML_CLIENT_ID"

    /**
     * @return arbitrary client ID for organization of output/written data
     */
    def clientId: String = sys.env(envClientId)

    /**
     * Name of the environment variable that configures the Elasticsearch URI
     * (you don't need to change this - this is just the name of the variable)
     */
    private val envElasticsearchURI = "FLOYOML_ELASTICSEARCH_URI"

    /**
     * @return URI of the Elasticsearch endpoint to write to
     */
    def elasticsearchURI: String = sys.env(envElasticsearchURI)
  }
  
  /**
   * Constants related to AWS S3
   */
  object S3 {
    object Common {
      val dataToProcessDirectory = "Process"
      val dataToTrainWithDirectory = "Training"
    }

    val kMeansDataDirectory = "KMeans"
  }

  /**
   * Constants related to Elasticsearch
   */
  object Elasticsearch {
    val kMeansTrainingIndex = "idx-kmeans-training-output"
    val kMeansProcessIndex = "idx-kmeans-process-output"

    val matrixFactorizationTrainingIndex = "idx-collab-training-output"
    val matrixFactorizationProcessIndex = "idx-collab-process-output"
  }

  /**
   * Constants related to the behaviour of the system
   */
  object Behaviour {
    val sparkMaster = "local[1]"

    /**
     * Constants related to system output
     */
    object Output {
      val kMeansPredictionsLocalPath = "/output/k-means/"
      val matrixFactorizationPredictionsLocalPath = "/output/matrix-factorization/"
    }
  }
}