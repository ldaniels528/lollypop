package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.HelpDoc.{CATEGORY_AGG_SORT_OPS, PARADIGM_DECLARATIVE}
import com.lollypop.language.OptionEnrichment
import com.lollypop.language.models.Expression
import com.lollypop.runtime.conversions.CSVConversion
import com.lollypop.runtime.datatypes.Inferences.{fromValue, resolveType}
import com.lollypop.runtime.datatypes.StringType
import com.lollypop.runtime.devices.RowCollectionZoo.createTempTable
import com.lollypop.runtime.devices._
import com.lollypop.runtime.instructions.functions.{FunctionCallParserE1, ScalarFunctionCall}
import com.lollypop.runtime.{LollypopVMAddOns, Scope}
import lollypop.io.IOCost

import java.util
import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
 * `TableArray` Transforms an array of an array of values into a table.
 * @example {{{
 *   def iostat(n, m) := TableArray(
 *                        (? iostat $n $m ?)
 *                          .drop(1)
 *                          .map(x => x.trim()
 *                                     .split("[ ]")
 *                                     .filter(x => not x.isEmpty())
 *   ))
 *   iostat(1, 5)
 * }}}
 * @param expression the [[Expression expression]] to transpose
 */
case class TableArray(expression: Expression) extends ScalarFunctionCall with RuntimeQueryable with Expression {
  override def name: String = "TableArray"

  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    @inline
    def getContainer(value: Any): Seq[Any] = value match {
      case a: Array[_] => a
      case c: util.Collection[_] => c.asScala.toSeq
      case s: Seq[_] => s
      case s: Iterable[_] => s.toSeq
      case z => expression.dieIllegalType(z)
    }

    // generate the dataframe
    expression.execute(scope) map { rawValue =>
      val dataGrid = getContainer(rawValue).map(getContainer)
      val dataSet = dataGrid.tail.map(_.map(CSVConversion.convert))
      val headers = dataGrid.headOption.map(_.map(StringType.convert)) || Nil
      val columns = for {
        (name, columnID) <- headers.zipWithIndex
        columnOfValues = dataSet.map(_.apply(columnID))
      } yield TableColumn(name, `type` = resolveType(columnOfValues.map(fromValue).toList))
      val (rmd, fmd) = (RowMetadata(), FieldMetadata())
      val rows = dataSet.zipWithIndex.map { case (values, id) =>
        Row(id = id, metadata = rmd, columns = columns, fields = (columns zip values).map {
          case (c, v) => Field(name = c.name, metadata = fmd.copy(isActive = v.nonEmpty), value = v)
        })
      }
      createTempTable(columns, rows)
    }
  }
}

object TableArray extends FunctionCallParserE1(
  name = "TableArray",
  category = CATEGORY_AGG_SORT_OPS,
  paradigm = PARADIGM_DECLARATIVE,
  description = "Transforms an array of an array of primitive values (Boolean, Int, String et al) into a table",
  examples = List(
    """|def iostat(n, m) := TableArray((? iostat $n $m ?).drop(1).map(x =>
       |  x.trim()
       |   .replaceAll("  ", " ")
       |   .split("[ ]")
       |   .filter(x => not x.isEmpty())
       |))
       |iostat(1, 5)
       |""".stripMargin,
    """|def ps() := TableArray((? ps aux ?).map(x =>
       |  x.trim()
       |   .replaceAll("  ", " ")
       |   .split("[ ]")
       |   .filter(x => not x.isEmpty())
       |))
       |select PID, `%CPU`, `%MEM`, STARTED, TIME, VSZ, RSS
       |from (ps())
       |order by `%CPU` desc
       |limit 5
       |""".stripMargin
  ))
  
