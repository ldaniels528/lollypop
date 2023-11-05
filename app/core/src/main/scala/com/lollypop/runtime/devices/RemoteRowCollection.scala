package com.lollypop.runtime.devices

import com.lollypop.database.clients.WebServiceClient
import com.lollypop.database.clients.WebServiceClient.LollypopResponseConversion
import com.lollypop.implicits.MagicImplicits
import com.lollypop.language.dieExpectedJSONObject
import com.lollypop.runtime.DatabaseObjectRef.DatabaseObjectRefRealization
import com.lollypop.runtime.ModelsJsonProtocol._
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.{DatabaseObjectNS, DatabaseObjectRef, INT_BYTES, ROWID, Scope}
import com.lollypop.util.ByteBufferHelper.DataTypeBuffer
import com.lollypop.util.JSONSupport.JsValueConversion
import com.lollypop.util.OptionHelper.OptionEnrichment
import com.lollypop.util.StringHelper._
import lollypop.io.IOCost
import spray.json.{JsObject, enrichAny}

import java.nio.ByteBuffer.{allocate, wrap}
import java.util.Base64
import scala.collection.concurrent.TrieMap

/**
 * Represents a Remote Row Collection (HTTP transport)
 * @example {{{
 *  val remoteStocks = q("//0.0.0.0/ldaniels.portfolio.stocks")
 *  select * from @remoteStocks limit 5
 * }}}
 * @param host the remote host
 * @param port the remote port
 * @param ns   the [[DatabaseObjectNS database object namespace]]
 */
case class RemoteRowCollection(host: String, port: Int, ns: DatabaseObjectNS)
  extends RowCollection with RowOrientedSupport {
  private val charSetName = "utf-8"
  private val $http = new WebServiceClient()
  private val columnCache = TrieMap[Unit, Seq[TableColumn]]()
  private var closed = false

  override def apply(rowID: ROWID): Row = {
    ($http.get(f"${baseURL("d")}/$rowID") match {
      case js: JsObject => Option(js.fields.map { case (name, jv) => name -> jv.unwrapJSON })
      case js => dieExpectedJSONObject(js)
    }).map(_.toRow(this)) ||
      Row(rowID, RowMetadata(isAllocated = false), columns, fields = columns.map(c => Field(c.name, FieldMetadata(c), value = None)))
  }

  override def insert(row: Row): IOCost = {
    val body = row.toMap.toJson.toString().getBytes(charSetName)
    $http.post(baseURL("d"), body).as[IOCost]
  }

  override def close(): Unit = closed = true

  override def columns: Seq[TableColumn] = columnCache.getOrElseUpdate((), $http.get(baseURL("col")).as[Seq[TableColumn]])

  override def encode: Array[Byte] = {
    val array = $http.getAsBytes(baseURL("dnl"))
    allocate(INT_BYTES + array.length).putInt(array.length).put(array).flipMe().array()
  }

  override def update(rowID: ROWID, row: Row): IOCost = {
    val body = row.toMap.toJson.toString().getBytes(charSetName)
    $http.put(f"${baseURL("d")}/$rowID", body)
    IOCost(updated = 1)
  }

  override def readField(rowID: ROWID, columnID: Int): Field = {
    val bytes = $http.getAsBytes(f"${baseURL("d")}/$rowID/$columnID")
    val column = columns(columnID)
    val value_? = Option(column.`type`.decode(wrap(bytes)))
    Field(column.name, FieldMetadata(), value_?)
  }

  override def updateField(rowID: ROWID, columnID: Int, newValue: Option[Any]): IOCost = {
    val body = Base64.getEncoder.encode(columns(columnID).`type`.encode(newValue))
    $http.put(f"${baseURL("d")}/$rowID/$columnID", body)
    IOCost(updated = 1)
  }

  override def getLength: ROWID = $http.get(baseURL("len")).as[Long]

  override def setLength(newSize: ROWID): IOCost = {
    $http.put(f"${baseURL("len")}?size=$newSize")
    IOCost(updated = 1)
  }

  override def sizeInBytes: Long = getLength * recordSize

  override def readFieldMetadata(rowID: ROWID, columnID: Int): FieldMetadata = {
    $http.get(f"${baseURL("fmd")}/$rowID/$columnID").as[FieldMetadata]
  }

  override def updateFieldMetadata(rowID: ROWID, columnID: Int, fmd: FieldMetadata): IOCost = {
    $http.put(f"${baseURL("fmd")}/$rowID/$columnID?fmd=${fmd.encode.toInt}")
    IOCost(updated = 1)
  }

  override def readRowMetadata(rowID: ROWID): RowMetadata = {
    $http.get(f"${baseURL("rmd")}/$rowID").as[RowMetadata]
  }

  override def updateRowMetadata(rowID: ROWID, rmd: RowMetadata): IOCost = {
    $http.put(f"${baseURL("rmd")}/$rowID?rmd=${rmd.encode.toInt}")
    IOCost(updated = 1)
  }

  private def baseURL(code: String): String = {
    s"http://$host:$port/$code/${ns.toSQL.replace('.', '/')}"
  }

}

object RemoteRowCollection {

  def getRemoteCollection(url: String)(implicit scope: Scope): Option[RemoteRowCollection] = {
    for {
      a <- url.indexOfOpt("//")
      b = a + 2
      c <- url.indexOfOpt("/", b)

      // get the host-port (e.g. "0.0.0.0:8888")
      (host, port) <- url.substring(b, c) ~> { hostPort =>
        hostPort.indexOfOpt(":").map(p => hostPort.splitAt(p) ~> { case (k, v) => k -> v.substring(1).toInt })
      }

      // get the table path (e.g. "ldaniels.portfolio.stocks")
      ref <- url.indexOfOpt("/", c).map { p => url.substring(p + 1) ~> { path => DatabaseObjectRef(path).toNS } }
    } yield RemoteRowCollection(host, port, ref)
  }

}