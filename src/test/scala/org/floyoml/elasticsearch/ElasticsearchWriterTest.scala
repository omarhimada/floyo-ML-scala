package org.floyoml.elasticsearch

import java.text.SimpleDateFormat
import java.util.{Date, SimpleTimeZone}

import com.sksamuel.elastic4s.IndexAndType
import org.junit._

import scala.collection.mutable.ListBuffer

@Test
class ElasticsearchWriterTest {

  private val dummyIndex = "idx-test"
  private val dummyElasticsearchURI = "http://localhost:1720"

  @Test
  def testRollingDateIndex(): Unit = {

    val esWriter = new ElasticsearchWriter[DocumentTest](
      uri = dummyElasticsearchURI,
      rollingDate = true,
      indexAndType = IndexAndType(dummyIndex, classOf[DocumentTest].getSimpleName),
      numberOfBulkDocumentsToWrite = 10,
      initialDocuments = ListBuffer.empty[DocumentTest]
    )

    val format = new SimpleDateFormat("yyyy.MM.dd")
    format.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"))
    val dateSuffix = format.format(new Date())

    val indexToWriteTo = esWriter.indexToWriteTo
    val constructedRollingDateIndex = s"$dummyIndex-$dateSuffix"

    // test rolling date index
    assert(indexToWriteTo == constructedRollingDateIndex)

    // todo
    assert(true)
  }
}