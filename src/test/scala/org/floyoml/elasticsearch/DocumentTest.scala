package org.floyoml.elasticsearch

import com.sksamuel.elastic4s.Indexable
import spray.json._

/**
 * Indexable dummy document for testing the Elasticsearch integration
 */
case class DocumentTest(id: String)
object DocumentTest {
  // DocumentTest can be serialized to JSON
  implicit val writer: JsonWriter[DocumentTest] = (document: DocumentTest) => {
    JsObject(
      "id" -> JsString(document.id),
    )
  }
  // DocumentTest can be indexed to Elasticsearch
  implicit val indexable: Indexable[DocumentTest] = (t: DocumentTest) => t.toJson.compactPrint
}