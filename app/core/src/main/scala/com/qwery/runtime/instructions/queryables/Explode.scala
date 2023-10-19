package com.qwery.runtime.instructions.queryables

import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAME, PARADIGM_DECLARATIVE}
import com.qwery.language.models.Expression
import com.qwery.language.models.Expression.implicits.RichAliasable
import com.qwery.runtime.Plastic.tupleToSeq
import com.qwery.runtime.datatypes._
import com.qwery.runtime.devices.RecordCollectionZoo.MapToRow
import com.qwery.runtime.devices.RowCollectionZoo._
import com.qwery.runtime.devices.{QMap, RowCollection, TableColumn}
import com.qwery.runtime.instructions.expressions.RuntimeExpression
import com.qwery.runtime.instructions.functions.{FunctionCallParserE1, ScalarFunctionCall}
import com.qwery.runtime.{QweryVM, Scope}
import com.qwery.util.JVMSupport.NormalizeAny
import qwery.io.IOCost

import scala.annotation.tailrec
import scala.reflect.ClassTag

/**
 * Separates the elements of a collection expression into a table expression.
 * @example {{{ explode(new `java.util.Date`()) }}}
 * @param expression the [[Expression expression]] to explode
 */
case class Explode(expression: Expression) extends ScalarFunctionCall with RuntimeQueryable with RuntimeExpression {

  override def evaluate()(implicit scope: Scope): RowCollection = search()._3

  override def search()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    val (scope0, cost0, result0) = QweryVM.execute(scope, expression)

    @tailrec
    def recurse(value: Any): RowCollection = value match {
      case null => null
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

  private def fromSeq[T](items: Seq[T])(implicit ct: ClassTag[T]): RowCollection = {
    val columnName = expression.getNameOrDie
    val columns = Seq(TableColumn(name = columnName, `type` = Inferences.fromClass(ct.runtimeClass)))
    implicit val out: RowCollection = createQueryResultTable(columns, fixedRowCount = items.size)
    items.foreach(item => out.insert(Map(columnName -> item).toRow))
    out
  }

}

object Explode extends FunctionCallParserE1(
  name = "explode",
  category = CATEGORY_DATAFRAME,
  paradigm = PARADIGM_DECLARATIVE,
  description = "Separates the elements of a collection expression into multiple rows, or the elements of map expr into multiple rows and columns.",
  examples = List(
    "explode(items: [1 to 5])",
    """|faces = explode(face: ["2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K", "A"])
       |suits = explode(suit: ["♠", "♦", "♥", "♣"])
       |deck = faces * suits
       |deck.shuffle()
       |@@deck limit 5
       |""".stripMargin
  ))
