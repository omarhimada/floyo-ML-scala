package org.floyoml.kmeans

import org.apache.kafka.streams.kstream.KStream
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.floyoml.input.Transaction
import org.floyoml.shared.Utility
import org.joda.time.DateTime

/**
 * Utility functions related to RFM (recency, frequency, monetary value)
 */
object RFMUtility {
  // minimum recency in a group of related transactions
  def minUnitRecency(grouped: Iterable[(Int, DateTime, Double, Double)]): Double =
    grouped.map(_._3).min

  // frequency (number of transactions in a group)
  def frequency(grouped: Iterable[(Int, DateTime, Double, Double)]): Double =
    grouped.size

  // sum the monetary value of a group of transactions
  def sumGroupedMonetaryValue(grouped: Iterable[(Int, DateTime, Double, Double)]): Double =
    grouped.map(_._4).sum

  /**
   * Featurize a grouped stream of (customerId, RFM)) to (customerId, vector)
   * @param filtered the RDD after the grouped transactions were filtered
   */
  def featurizeRDD(filtered: RDD[(Int, Double, Double, Double)]): RDD[(Int, Vector)] =
    filtered
      .map(f =>
        (f._1, RFMUtility.featurizeForRFM(Iterable[Double](f._2, f._3, f._4))))

  /**
   * Transform a KStream of (customerId, (RFM)) to (customerId, vector) tuples, as input for prediction
   */
  def transformStreamAndPredict(toTransform: KStream[Double, Transaction]): KStream[Double, (Int, Vector)] =
    toTransform
      .mapValues(t => (t.customerId, featurizeForRFM(Iterable[Double](t.date.getDayOfYear.toDouble, t.unitRecency, t.unitMonetary))))

  /**
   * Use a KStream of (customerId, vector) to make predictions using a persisted model
   */
  def predictStream(toPredict: KStream[Double, (Int, Vector)], persistedKMeansModel: KMeansModel): KStream[Double, (Int, Int)] =
    toPredict
      .mapValues(t => (t._1, persistedKMeansModel.predict(t._2)))

  /**
   * Transform an RDD of (customerId, (RFM)) to (customerId, vector) tuples, and input
   * for prediction to get an RDD of (customerId, cluster)
   */
  def transformRDDAndPredict(filtered: RDD[(Int, Double, Double, Double)], persistedKMeansModel: KMeansModel): RDD[(Int, Int)] =
    featurizeRDD(filtered)
      .map(f =>
        (f._1,
        persistedKMeansModel.predict(f._2)))

  /**
   * Take a grouped RDD of (customerId: Int, recency: Double, frequency: Double, monetary: Double)
   * and filter out the 95th + 5th percentiles
   * @param grouped RDD of transactions grouped by customer ID
   * @return RDD of (customerId: Int, recency: Double, frequency: Double, monetary: Double)
   */
  def filterGroupedForRFM(grouped: RDD[(Int, Double, Double, Double)]): RDD[(Int, Double, Double, Double)] = {
    val recencies = grouped.map(t => t._2)
    val frequencies = grouped.map(t => t._3)
    val monetaries = grouped.map(t => t._4)

    // 95th percentile recency
    val topOutlierRecency = Utility.computeTopPercentile(recencies)
    // 5th percentile recency
    val bottomOutlierRecency = Utility.computeBottomPercentile(recencies)

    // 95th percentile frequency
    val topOutlierFrequency = Utility.computeTopPercentile(frequencies)
    // 5th percentile frequency
    val bottomOutlierFrequency = Utility.computeBottomPercentile(frequencies)

    // 95th percentile monetary
    val topOutlierMonetary = Utility.computeTopPercentile(monetaries)
    // 5th percentile monetary
    val bottomOutlierMonetary = Utility.computeBottomPercentile(monetaries)

    // remove top and bottom outliers for RFM
    grouped.filter(g =>
      g._2 < topOutlierRecency && g._2 > bottomOutlierRecency &&
        g._3 < topOutlierFrequency && g._3 > bottomOutlierFrequency &&
        g._4 < topOutlierMonetary && g._4 > bottomOutlierMonetary)
  }

  /**
   * Group an RDD of transactions by customerId (param 0)
   * @param rdd RDD of transactions as (customerId: Int, date: DateTime, unitRecency: Double, unitMonetary: Double)
   * @return RDD of grouped, transformed transactions as (customerId: Int, recency: Double, frequency: Double, monetary: Double)
   */
  def groupRddByCustomerIdAndTransform(rdd: RDD[(Int, DateTime, Double, Double)]): RDD[(Int, Double, Double, Double)] =
    rdd
      // RDD[(customerId, Iterable(customerId, uR, uM))]
      .groupBy(_._1)
      .map(grouped =>
        // RDD[(customerId: Int, R: Double, F: Double, M: Double)]
        (grouped._1,
          // recency: min(time since last transaction)
          RFMUtility.minUnitRecency(grouped._2),
          // frequency: count(transactions)
          RFMUtility.frequency(grouped._2),
          // monetary: sum(transaction value)
          RFMUtility.sumGroupedMonetaryValue(grouped._2)))

  // For min-hash
  private val _dimensions: Int = 1000
  private val tf = new HashingTF(_dimensions)

  /**
   * Transform (min-hash) a group of relevant transactions (R, F, M) to a Vector[Double]
   * @param transactions recency, frequency and monetary value for a customer
   */
  def featurizeForRFM(transactions: Iterable[Double]): Vector = tf.transform(transactions.toSeq)
}
