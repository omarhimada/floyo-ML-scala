package org.floyoml.output

import com.sksamuel.elastic4s.Indexable
import spray.json._

/**
 * Output rated transaction (e.g.: eCommerce sale) ('Rating' from ALS)
 */
case class RatedTransaction(customerId: Int, sku: Int, rating: Double)
object RatedTransaction {
  // ClusterPrediction can be serialized to JSON
  implicit val writer: JsonWriter[RatedTransaction] = (rating: RatedTransaction) => {
    JsObject(
      "customerId" -> JsString(rating.customerId.toString),
      "sku" -> JsString(rating.sku.toString),
      "rating" -> JsString(rating.rating.toString)
    )
  }
  // ClusterPrediction can be indexed to Elasticsearch
  implicit val indexable: Indexable[RatedTransaction] = (t: RatedTransaction) => t.toJson.compactPrint
}