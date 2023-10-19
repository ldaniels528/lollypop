package com.qwery.runtime.devices

import com.qwery.runtime._
import com.qwery.runtime.datatypes.TableType
import com.qwery.runtime.devices.RowCollectionZoo.createQueryResultTable
import com.qwery.runtime.instructions.queryables.TableRendering
import com.qwery.util.StringRenderHelper

/**
 * Represents a database row
 * @param id       the row's unique [[ROWID row identifier]]
 * @param metadata the [[RowMetadata row metadata]]
 * @param columns  the collection of [[TableColumn columns]]
 * @param fields   the collection of [[Field fields]]
 */
case class Row(id: ROWID, metadata: RowMetadata, columns: Seq[TableColumn], fields: Seq[Field])
  extends QMap[String, Any] with TableRendering {

  override def apply(name: String): Any = fields.find(_.name == name).flatMap(_.value).orNull

  override def -(key: String): collection.Map[String, Any] = toMap.filterNot { case (k, _) => key == k }

  override def -(key1: String, key2: String, keys: String*): collection.Map[String, Any] = {
    val removeKeys = Set(key1, key2) ++ keys.toSet
    toMap.filterNot { case (k, _) => removeKeys.contains(k) }
  }

  override def iterator: Iterator[(String, Any)] = toMap.iterator

  override def contains(name: String): Boolean = columns.exists(_.name == name)

  override def get(name: String): Option[Any] = fields.find(_.name == name).flatMap(_.value)

  /**
   * Retrieves a field by column ID
   * @param columnID the column ID
   * @return the [[Field]]
   */
  def getField(columnID: Int): Field = fields(columnID)

  /**
   * Retrieves a field by column name
   * @param name the name of the field
   * @return the option of a [[Field]]
   */
  def getField(name: String): Option[Field] = fields.find(_.name == name)

  /**
   * @return a [[Map hashMap representation]] of the row
   */
  def toMap: Map[String, Any] = Map(fields.flatMap(f => f.value.map(f.name -> _)).toList: _*)

  /**
   * @return a [[Map hashMap representation]] of the row
   */
  def toMap(recursive: Boolean): Map[String, Any] = {
    Map(fields.flatMap(f => f.value.map {
      case d: RowCollection if recursive => f.name -> d.toMapGraph
      case v => f.name -> v
    }).toList: _*)
  }

  override def toString: String = StringRenderHelper.toRowString(this)

  override def toTable(implicit scope: Scope): RowCollection = {
    val out = createQueryResultTable(columns)
    out.insert(this)
    out
  }

  override def toTableType: TableType = TableType(columns)

}
