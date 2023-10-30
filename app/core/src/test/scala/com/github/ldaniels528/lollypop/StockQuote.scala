package com.github.ldaniels528.lollypop

import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.{ColumnInfo, ROWID}

import scala.annotation.meta.field
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Random

case class StockQuote(@(ColumnInfo@field)(maxSize = 8) symbol: String,
                      @(ColumnInfo@field)(maxSize = 8) exchange: String,
                      lastSale: Double,
                      lastSaleTime: Long)

object StockQuote {
  private val random = new Random()

  def randomExchange: String = {
    val exchanges = Seq("AMEX", "NASDAQ", "OTCBB", "NYSE")
    exchanges(random.nextInt(exchanges.size))
  }

  def randomQuote: StockQuote = StockQuote(randomSymbol, randomExchange, randomPrice, randomDate)

  def randomQuote(minLength: Int, maxLength: Int): StockQuote = {
    StockQuote(randomSymbol(minLength, maxLength), randomExchange, randomPrice, randomDate)
  }

  def randomDate: Long = 1603486147408L + random.nextInt(20).days.toMillis

  def randomPrice: Double = random.nextDouble() * random.nextInt(1000)

  def randomSummary: String = {
    val length = 240
    val chars = 'A' to 'Z'
    String.valueOf((0 until length).map(_ => chars(random.nextInt(chars.length))).toArray)
  }

  def randomSymbol: String = randomSymbol(3, 6)

  def randomSymbol(minLength: Int, maxLength: Int): String = {
    val length = minLength + random.nextInt(maxLength - minLength)
    val chars = 'A' to 'Z'
    String.valueOf((0 until length).map(_ => chars(random.nextInt(chars.length))).toArray)
  }

  @tailrec
  def randomURID[A <: Product : ClassTag](device: RowCollection): ROWID = {
    val offset: ROWID = random.nextInt(device.getLength.toInt)
    if (device.readRowMetadata(offset).isDeleted) randomURID(device) else offset
  }

}

