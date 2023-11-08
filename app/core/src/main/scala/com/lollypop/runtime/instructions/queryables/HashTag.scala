package com.lollypop.runtime.instructions.queryables

import com.lollypop.implicits.MagicImplicits
import com.lollypop.language.HelpDoc.{CATEGORY_DATAFRAMES_IO, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.language.models.{Expression, FieldRef, NamedExpression}
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.Scope
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.devices.RowCollectionZoo.{createQueryResultTable, createTempTable}
import com.lollypop.runtime.instructions.expressions.ArrayLiteral
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

import scala.language.{existentials, postfixOps}

/**
 * Represents an hashtag expression (e.g. "stocks#symbol" or "stocks#[symbol, exchange]" or "ns('Stocks')#symbol")
 * @param host the [[Expression host table]]
 * @param tags the [[Expression field or array of fields]] being referenced
 */
case class HashTag(host: Expression, tags: Expression) extends RuntimeQueryable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    host.execute(scope) match {
      case (scopeA, costA, rc: RowCollection) =>
        createColumnarTable(rc, getColumnNames, cost0 = costA) ~> { case (c, r) => (scope, c, r)}
      case (_, _, other) => host.dieIllegalType(other)
    }
  }

  override def toSQL: String = s"${host.wrapSQL}#${tags.toSQL}"

  private def createColumnarTable(rc: RowCollection, columnNames: List[String], cost0: IOCost): (IOCost, RowCollection) = {
    val columns = for {column <- rc.columns if columnNames.contains(column.name)} yield column
    val out = if (rc.isMemoryResident) createQueryResultTable(columns) else createTempTable(columns)
    val cost = rc.foldLeft(cost0) { case (cost, row) =>
      val outRow = row.copy(columns = columns, fields = row.fields.filter(f => columnNames.contains(f.name)))
      cost ++ out.insert(outRow)
    }
    cost -> out
  }

  private def getColumnName(expression: Expression): String = expression match {
    case FieldRef(name) => name
    case other => other.dieIllegalType()
  }

  private def getColumnNames: List[String] = tags match {
    case ArrayLiteral(fields) => fields.map(getColumnName)
    case FieldRef(name) => List(name)
    case other => other.dieIllegalType()
  }

}

object HashTag extends ExpressionChainParser {
  private val keyword = "#"

  override def parseExpressionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Expression] = {
    if (ts nextIf keyword) NamedExpression.parseExpression(ts).map(HashTag(host, _)) ?? ts.dieExpectedExpression() else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_DATAFRAMES_IO,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = s"`dataFrame`$keyword`column`",
    description = "Returns a column slice of a data frame",
    example =
      s"""|declare table stocks(symbol: String(4), exchange: String(6), lastSale: Double, lastSaleTime: DateTime)
          |containing (
          ||---------------------------------------------------------|
          || symbol | exchange | lastSale | lastSaleTime             |
          ||---------------------------------------------------------|
          || NQOO   | AMEX     | 190.1432 | 2023-08-10T01:44:20.075Z |
          || LVMM   | NASDAQ   | 164.2654 | 2023-08-10T01:44:20.092Z |
          || VQLJ   | AMEX     |  160.753 | 2023-08-10T01:44:20.093Z |
          || LRBJ   | OTCBB    |  64.0764 | 2023-08-10T01:44:20.095Z |
          || QFHM   | AMEX     | 148.6447 | 2023-08-10T01:44:20.096Z |
          ||---------------------------------------------------------|
          |)
          |stocks#[symbol, lastSale]
          |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}
