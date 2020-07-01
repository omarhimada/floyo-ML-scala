package org.floyoml.elasticsearch

import java.text.SimpleDateFormat
import java.util.{Date, SimpleTimeZone}
import scala.collection.mutable.ListBuffer

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties}
import com.sksamuel.elastic4s.indexes.IndexRequest
import com.sksamuel.elastic4s.{IndexAndType, Indexable, RefreshPolicy}

class ElasticsearchWriter[T: Indexable](
  uri: String,
  rollingDate: Boolean,
  indexAndType: IndexAndType,
  numberOfBulkDocumentsToWrite: Int = 5,
  initialDocuments: ListBuffer[T] = ListBuffer.empty[T]) {

  private val _client: ElasticClient = ElasticClient(ElasticProperties(uri))

  /**
   * As 'Write' is called this list buffer is appended to, until it reaches _numberOfBulkDocumentsToWrite,
   * at which point a bulk request is triggered to Elasticsearch and the list is emptied
   */
  private var _documents: ListBuffer[T] = initialDocuments

  /**
   * Construct the name of the index to write the document(s) to
   * @return Index's name
   */
  def indexToWriteTo: String = if (rollingDate) rollingDateIndex(indexAndType.index) else indexAndType.index

  /**
   * Used when rollingDate == true to construct the index
   * @param i The base index string
   * @return Formatted index with the pattern "myIndex-yyyy.MM.dd"
   */
  private def rollingDateIndex(i: String): String = s"$i$rollingDateIndexSuffix"

  /**
   * Used when rollingDate == true to construct the index's date suffix
   * @return Current UTC date as a string in the format "-yyyy.MM.dd"
   */
  private def rollingDateIndexSuffix: String = {
    val format = new SimpleDateFormat("yyyy.MM.dd")
    format.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"))
    val f = format.format(new Date())
    s"-$f"
  }

  /**
   * Prepares many IndexRequests for usage in the Bulk request
   */
  def bulkFromBuffer: Iterable[IndexRequest] =
    for (d <- _documents) yield indexInto(indexToWriteTo / indexAndType.`type`) doc d

  def write(documents: Seq[T]): Unit = {
    // append the document(s) to the document buffer
    documents.foreach(doc => _documents += doc)

    // check if the document buffer has reached capacity
    if (_documents.length > numberOfBulkDocumentsToWrite) {
      // execute a bulk request
      _client.execute {
        bulk(
          bulkFromBuffer
        ).refresh(RefreshPolicy.WaitFor)
      }.await

      // empty the document buffer
      _documents.clear

      // todo: search
      /*val response: Response[SearchResponse] = _client.execute {
        search(indexAndType.index).matchQuery("capital", "ulaanbaatar")
      }.await*/

      // debugging: prints out the original json
      //println(response.result.hits.hits.head.sourceAsString)

      _client.close
    }
  }
}
