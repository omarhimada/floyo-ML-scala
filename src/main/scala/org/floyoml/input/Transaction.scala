package org.floyoml.input

import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import org.joda.time.{DateTime, Days}
import spray.json._

/**
 * Input transaction (e.g.: eCommerce sale)
 */
case class Transaction(customerId: Int,
                       sku: String,
                       quantity: Int,
                       date: DateTime,
                       unitPrice: Double) {

  /**
   * Unit of recency for RFM
   * @return number of days since today as a Double
   */
  def unitRecency: Double = Days.daysBetween(DateTime.now, date).getDays.toDouble

  /**
   * Unit of monetary value for RFM
   * @return total value of this transaction
   */
  def unitMonetary: Double = quantity * unitPrice

  // For serializing to Elasticsearch
  override def toString: String = s"${customerId}_$sku-$unitPrice-${quantity}_${date.toString}"
}
object Transaction {
  // For parsing to DateTime
  implicit val parser: DateTimeFormatter = ISODateTimeFormat.basicDateTime

  // Transaction can be parsed from JSON
  implicit val reader: JsonReader[Transaction] = (value: JsValue) => {
    value.asJsObject.getFields("customerId", "sku", "quantity", "date", "unitPrice") match {
      case Seq(JsNumber(customerId), JsString(sku), JsNumber(quantity), JsString(date), JsNumber(unitPrice)) =>
        Transaction(
          customerId.toInt,
          sku,
          quantity.toInt,
          parser.parseDateTime(date),
          unitPrice.toInt)

      case _ => throw DeserializationException("Invalid transaction schema")
    }
  }
}