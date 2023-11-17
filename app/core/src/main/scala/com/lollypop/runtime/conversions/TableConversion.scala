package com.lollypop.runtime.conversions

import com.lollypop.die
import com.lollypop.language.dieIllegalType
import com.lollypop.runtime.LollypopVM.rootScope
import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes.Inferences
import com.lollypop.runtime.datatypes.Inferences.fromValue
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.devices.RowCollectionZoo._
import com.lollypop.runtime.devices._
import com.lollypop.runtime.instructions.queryables.TableRendering

import scala.language.postfixOps

/**
 * Table Conversion
 */
trait TableConversion extends Conversion {

  override def convert(value: Any): RowCollection = value match {
    case a: Array[_] => convertSeqToTable(a)
    case s: Seq[_] => convertSeqToTable(s)
    case s: Scope => s.toRowCollection
    case t: TableRendering => t.toTable(rootScope)
    case p: Product => p.toRowCollection
    case m: QMap[_, _] => m.toKeyValueCollection
    case x => dieIllegalType(x)
  }

  def convertTupleToTable(columnName: String, value: Any): RowCollection = {
    val device = createTempTable(columns = Seq(TableColumn(columnName, `type` = fromValue(value))), fixedRowCount = 1)
    device.insert(Map(columnName -> value).toRow(device))
    device
  }

  def convertSeqToTable(collection: Seq[_]): RowCollection = {
    // determine the data types of the dictionary entries
    val rawColumns = collection map {
      case rc: RowCollection => rc.columns
      case row: Row => row.columns
      case dict: QMap[String, Any] =>
        dict.toSeq map { case (name, value) => TableColumn(name, `type` = Inferences.fromValue(value)) }
      case other => die(s"Expected a dictionary object, got ${Option(other).map(_.getClass.getName).orNull}")
    }

    // determine the best fit for each generated column type
    val columns = rawColumns.flatten.groupBy(_.name).toSeq.map { case (name, columns) =>
      TableColumn(name, `type` = Inferences.resolveType(columns.map(_.`type`): _*))
    }

    // write the data to the table
    val device = FileRowCollection(columns)
    val (fmd, rmd) = (FieldMetadata(), RowMetadata())
    collection foreach {
      case rc: RowCollection => device.insert(rc)
      case dict: QMap[String, Any] =>
        val row = Row(device.getLength, rmd, columns, fields = columns map { column =>
          Field(column.name, fmd, value = dict.get(column.name))
        })
        device.insert(row)
      case x => dieIllegalType(x)
    }
    device
  }

}

/**
 * Table Conversion Singleton
 */
object TableConversion extends TableConversion