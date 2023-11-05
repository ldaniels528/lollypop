package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.HelpDoc.{CATEGORY_AGG_SORT_OPS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.models.Expression.implicits.RichAliasable
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.devices.RowCollectionZoo._
import com.lollypop.runtime.devices.{QMap, Row, RowCollection, TableColumn}
import com.lollypop.runtime.instructions.functions.{FunctionCallParserE1, ScalarFunctionCall}
import com.lollypop.runtime.plastics.Tuples.tupleToSeq
import com.lollypop.runtime.{LollypopVM, Scope}
import com.lollypop.util.JVMSupport.NormalizeAny
import lollypop.io.IOCost

import scala.annotation.tailrec
import scala.reflect.ClassTag

/**
 * Separates the elements of a collection expression into a table expression.
 * @example {{{ transpose(new `java.util.Date`()) }}}
 * @param expression the [[Expression expression]] to transpose
 */
case class Transpose(expression: Expression) extends ScalarFunctionCall with RuntimeQueryable with Expression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    val (scope0, cost0, result0) = LollypopVM.execute(scope, expression)

    @tailrec
    def recurse(value: Any): RowCollection = value match {
      case null => null
      case m: Matrix => m.transpose.toTable
      case r: Row => fromRow(r)
      case r: RowCollection => fromTable(r)
      case a: Array[_] => fromArray(a)
      case s: Seq[_] => fromSeq(s.asInstanceOf[Seq[AnyRef]])
      case s: Set[_] => recurse(s.toSeq)
      case m: QMap[_, _] => m.toKeyValueCollection
      case p: Product => fromProduct(p)
      case t if tupleToSeq(t).nonEmpty => fromSeq(tupleToSeq(t) getOrElse Nil)
      case x => fromPojo(x)
    }

    val result1 = recurse(result0.normalize)
    (scope0, cost0, result1)
  }

  private def fromArray[T](items: Array[T]): RowCollection = {
    val columnName = expression.getNameOrDie
    val columns = Seq(TableColumn(name = columnName, `type` = Inferences.fromClass(items.getClass.getComponentType)))
    implicit val out: RowCollection = createQueryResultTable(columns, fixedRowCount = items.length)
    items.foreach(item => out.insert(Map(columnName -> item).toRow))
    out
  }

  private def fromPojo(instance: Any): RowCollection = {
    val values = Map((
      for {
        m <- instance.getClass.getMethods if m.getParameterTypes.isEmpty && m.getName.startsWith("get")
        value = m.invoke(instance)
        name = m.getName.drop(3) match {
          case "Class" => "_class"
          case other => other.head.toLower + other.tail
        }
      } yield name -> value): _*)
    values.toKeyValueCollection
  }

  private def fromProduct(p: Product): RowCollection = {
    Map(p.productElementNames.toSeq zip p.productIterator: _*).toKeyValueCollection
  }

  private def fromRow(row: Row): RowCollection = {
    val newColumns: Seq[TableColumn] = Seq(
      TableColumn(name = "name", `type` = StringType(maxSizeInBytes = 65536, isExternal = true)),
      TableColumn(name = "value", `type` = StringType(maxSizeInBytes = 65536, isExternal = true))
    )
    val out = createTempTable(newColumns)
    row.columns foreach { column =>
      out.insert(Map("name" -> column.name, "value" -> row.get(column.name).orNull).toRow(out))
    }
    out
  }

  private def fromSeq[T](items: Seq[T])(implicit ct: ClassTag[T]): RowCollection = {
    val columnName = expression.getNameOrDie
    val columns = Seq(TableColumn(name = columnName, `type` = Inferences.fromClass(ct.runtimeClass)))
    implicit val out: RowCollection = createQueryResultTable(columns, fixedRowCount = items.size)
    items.foreach(item => out.insert(Map(columnName -> item).toRow))
    out
  }

  private def fromTable(rc: RowCollection): RowCollection = {
    val newColumns: Seq[TableColumn] = Seq(
      TableColumn(name = "name", `type` = StringType(maxSizeInBytes = 65536, isExternal = true)),
      TableColumn(name = "value", `type` = StringType(maxSizeInBytes = 65536, isExternal = true))
    )
    val out = createTempTable(newColumns)
    rc.foreach { row => out.insert(fromRow(row)) }
    out
  }

}

object Transpose extends FunctionCallParserE1(
  name = "transpose",
  category = CATEGORY_AGG_SORT_OPS,
  paradigm = PARADIGM_DECLARATIVE,
  description = "Makes columns into rows and rows into columns. The function returns a table with the rows and columns transposed.",
  examples = List(
    "transpose(items: [1 to 5])",
    """|faces = transpose(face: ["2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K", "A"])
       |suits = transpose(suit: ["♠", "♦", "♥", "♣"])
       |deck = faces * suits
       |deck.shuffle()
       |@deck limit 5
       |""".stripMargin,
    "transpose(help('select'))",
    """|transpose(new Matrix([
       |  [1.0, 2.0, 3.0],
       |  [4.0, 5.0, 6.0],
       |  [7.0, 8.0, 9.0]
       |]))
       |""".stripMargin
  ))
