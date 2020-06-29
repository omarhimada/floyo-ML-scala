package org.floyoml.kmeans

import com.sksamuel.elastic4s.Indexable
import spray.json._

case class ClusterPrediction(message: String, cluster: Int)
object ClusterPrediction {
  // ClusterPrediction can be serialized to JSON
  implicit val writer: JsonWriter[ClusterPrediction] = (clusterPrediction: ClusterPrediction) => {
    JsObject(
      "message" -> JsString(clusterPrediction.message),
      "cluster" -> JsString(clusterPrediction.cluster.toString)
    )
  }
  // ClusterPrediction can be indexed to Elasticsearch
  implicit val indexable: Indexable[ClusterPrediction] = (t: ClusterPrediction) => t.toJson.compactPrint
}

