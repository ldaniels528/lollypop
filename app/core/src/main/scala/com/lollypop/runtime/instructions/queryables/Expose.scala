package com.lollypop.runtime.instructions.queryables

import com.lollypop.die
import com.lollypop.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression.implicits.LifestyleExpressionsAny
import com.lollypop.language.models.{Expression, Instruction}
import com.lollypop.runtime.datatypes.{BooleanType, StringType, TableType}
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.devices.RowCollectionZoo.createQueryResultTable
import com.lollypop.runtime.devices.{QMap, RowCollection, TableColumn}
import com.lollypop.runtime.instructions.conditions.Matches
import com.lollypop.runtime.instructions.expressions.TableExpression
import com.lollypop.runtime.instructions.functions.{AnonymousFunction, FunctionCallParserE1, ScalarFunctionCall}
import com.lollypop.runtime.instructions.queryables.Expose.exposeMatch
import com.lollypop.runtime.{LollypopVM, Scope}
import com.lollypop.util.JVMSupport.NormalizeAny
import com.lollypop.util.StringRenderHelper.StringRenderer
import lollypop.io.IOCost

/**
 * Advanced Pattern Matching - Exposes the components of a `matches` expression
 * @param expression the [[Expression expression]] to expose
 * @example {{{
 *   set response = { id: 5678, symbol: "DOG", exchange: "NYSE", "lastSale": 90.67 }
 *   response matches { id: isUUID, symbol: isString, exchange: isString, lastSale: isNumber }
 * }}}
 */
case class Expose(expression: Expression) extends ScalarFunctionCall with RuntimeQueryable with TableExpression {

  override def search()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    expression match {
      case Matches(source, pattern) =>
        val rows = exposeMatch(src = LollypopVM.execute(scope, source)._3, pattern = LollypopVM.execute(scope, pattern)._3)
        val out = createQueryResultTable(returnType.columns)
        val cost = out.insert(rows.map(_.toRow(out)))
        (scope, cost, out)
      case other => other.dieIllegalType()
    }
  }

  override def returnType: TableType = TableType(columns = Seq(
    TableColumn(name = "expression", `type` = StringType),
    TableColumn(name = "value", `type` = StringType),
    TableColumn(name = "result", `type` = BooleanType)
  ))

}

object Expose extends FunctionCallParserE1(
  name = "expose",
  category = CATEGORY_FILTER_MATCH_OPS,
  paradigm = PARADIGM_DECLARATIVE,
  description = "Exposes the components of a `matches` expression",
  example =
    """|isString = v => v.isString()
       |isUUID = v => v.isUUID()
       |isNumber = v => v.isNumber()
       |response = { id: "a891ee9b-6667-40fc-9ed1-a129d04c8b6d", symbol: "ABC", exchange: "NYSE", lastSale: "35.76" }
       |expose(response matches { id: isUUID, symbol: isString, exchange: isString, lastSale: isNumber })
       |""".stripMargin) {

  def exposeMatch(src: Any, pattern: Any)(implicit scope: Scope): List[QMap[String, Any]] = {
    (src.normalizeArrays, pattern.normalizeArrays) match {
      case (a: QMap[String, _], b: QMap[String, _]) => exposeMatchMap(a, b)
      case (a: QMap[String, _], b: Seq[_]) => exposeMatchSeq(a.toSeq, b)
      case (a: Seq[_], b: QMap[String, _]) => exposeMatchSeq(a, b.toSeq)
      case (a: Seq[_], b: Seq[_]) => exposeMatchSeq(a, b)
      case (a, b: AnonymousFunction) => List(exposeAnonymousFunction(a, b))
      case (a, b) => List(Map("expression" -> s"${a.toSQL} == ${b.toSQL}", "result" -> (a == b)))
    }
  }

  private def exposeAnonymousFunction(a: Any, b: AnonymousFunction)(implicit scope: Scope): Map[String, Any] = {
    val value = a.v
    val fx = b.call(List(value))
    Map("expression" -> b.toSQL, "value" -> value.toSQL, "result" -> (LollypopVM.execute(scope, fx)._3 == true))
  }

  private def exposeMatchMap(src: QMap[String, _], pattern: QMap[String, _])(implicit scope: Scope): List[QMap[String, Any]] = {
    if (src.keys != pattern.keys) Nil
    else {
      val rows = pattern.toList map {
        case (name: String, af: AnonymousFunction) =>
          src.get(name).toList.map(exposeAnonymousFunction(_, af))
        case (name: String, value) =>
          val rows = src.get(name).toList.map(exposeMatch(_, value))
          reduce(rows)
        case (name, _) => die(s"${name.toSQL} is not of type String.")
      }
      reduce(rows)
    }
  }

  private def exposeMatchSeq(src: Seq[_], pattern: Seq[_])(implicit scope: Scope): List[QMap[String, Any]] = {
    if (src.size != pattern.size) Nil
    else {
      val rows = (src zip pattern).toList map { case (a, b) => exposeMatch(a, b) }
      reduce(rows)
    }
  }

  private def reduce(rows: List[List[QMap[String, Any]]]): List[QMap[String, Any]] = {
    rows.reduceLeft[List[QMap[String, Any]]] { case (a, b) => a ::: b }
  }

  /**
   * SQL Decompiler Helper
   * @param opCode the [[AnyRef opCode]] to decompile
   */
  final implicit class SQLDecompilerHelper(val opCode: Any) extends AnyVal {
    def toSQL: String = opCode match {
      case d: Instruction => d.toSQL
      case x => x.renderAsJson
    }
  }

}