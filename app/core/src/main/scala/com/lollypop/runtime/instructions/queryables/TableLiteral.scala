package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.Token.TableToken
import com.lollypop.language.models.{Expression, FieldRef, Literal}
import com.lollypop.language.{ExpressionParser, HelpDoc, LifestyleExpressionsAny, QueryableParser, SQLCompiler, TokenStream}
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.devices.RowCollectionZoo._
import com.lollypop.runtime.devices.{RecordStructure, RowCollection, TableColumn}
import com.lollypop.runtime.instructions.expressions._
import com.lollypop.util.DateHelper
import com.lollypop.util.Tabulator.tabulate
import lollypop.io.IOCost
import lollypop.lang.Null

import scala.util.{Success, Try}

/**
 * Represents a Table literal
 * @param columns the table columns
 * @param value   the table contents; a list of a list of [[Expression expressions]]
 * @example {{{
 * |--------------------------------------------------------------------------|
 * | exchange | symbol | codes          | lastSale | lastSaleTime             |
 * |--------------------------------------------------------------------------|
 * | NYSE     | ABC    | ["ABC"]        |    56.98 | 2021-09-13T04:42:44.812Z |
 * | NASDAQ   | GE     | ["XYZ"]        |    83.13 | 2021-09-13T04:42:44.812Z |
 * | OTCBB    | GMTQ   | ["ABC", "XYZ"] |   0.1111 | 2021-09-13T04:42:44.812Z |
 * |--------------------------------------------------------------------------|
 * }}}
 */
case class TableLiteral(columns: List[TableColumn], value: List[List[Expression]])
  extends AbstractTableExpression(columns) with RecordStructure with RuntimeQueryable with RuntimeExpression
    with Literal with TableRendering with TableExpression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    val device = createTempTable(columns, fixedRowCount = value.size)
    val columnNames = columns.map(_.name)
    val cost = (for {
      values <- value.map(_.map(_.execute(scope)._3))
      mapping = Map(columnNames zip values: _*)
    } yield device.insert(mapping.toRow(device))) reduce (_ ++ _)
    (scope, cost, device)
  }

  override def returnType: TableType = toTableType

  override def toSQL: String = {
    val sql = tabulate(headers = columns.map(_.name), rows = value.map(_.map(x => Some(x.toSQL))), limit = Int.MaxValue).mkString("\n")
    s"\n$sql\n"
  }

  override def toTable(implicit scope: Scope): RowCollection = execute()(scope)._3

  override def toTableType: TableType = TableType(columns)

}

object TableLiteral extends QueryableParser with ExpressionParser {

  def apply(rowsList: List[List[String]]): TableLiteral = {
    val compiler = LollypopCompiler()

    case class Unsupported(expr: Expression) extends Expression

    def cleanup(expression: Expression): Expression = expression match {
      case FieldRef(name) => name.v
      case ArrayLiteral(values) =>
        val filteredValues = values.map(cleanup)
        if (filteredValues.exists(_.isInstanceOf[Unsupported])) Unsupported(expression) else ArrayLiteral(filteredValues)
      case Dictionary(values) =>
        val filteredValues = values.map { case (name, value) => name -> cleanup(value) }
        if (filteredValues.exists(_._2.isInstanceOf[Unsupported])) Unsupported(expression) else Dictionary(filteredValues)
      case l: Literal => l
      case expr => Unsupported(expr)
    }

    // translates a cell (text) into an expression
    def translate(sql: String): (Expression, DataType) = {
      val model = sql match {
        case s if s.isEmpty => Null()
        case "false" => false.v
        case "true" => true.v
        case s if s.matches("^[+-]?(\\d+)$") => s.toLong.v
        case s if s.matches("^[+-]?(\\d*\\.?\\d+|\\d+\\.?\\d*)$") => s.toDouble.v
        case s if s.matches(UUID_REGEX) => Literal(UUIDType.convert(s))
        case s if s.matches(ISO_8601_REGEX) => Literal(DateHelper(s))
        case s =>
          Try(compiler.compile(s)) match {
            case Success(expr: Expression) =>
              cleanup(expr) match {
                case _: Unsupported => s.v
                case z => z
              }
            case _ => s.v
          }
      }
      model -> Inferences.inferType(model)
    }

    // generate the columns, data (list of list of expressions)
    val valuesAndTypes = rowsList.tail.map(_.map(s => translate(s.trim)))
    val (contents, types) = (valuesAndTypes.map(_.map(_._1)), valuesAndTypes.map(_.map(_._2)))

    // resolve each column down to a single type
    val columnCount = if (types.nonEmpty) types.map(_.size).max else 0
    val resolvedTypes = for {
      col <- 0 until columnCount
    } yield types.map(_.apply(col)).reduce((a, b) => Inferences.resolveType(a, b))

    // define the columns
    val columns = for {
      names <- rowsList.headOption.toList
      (name, resolvedType) <- names zip resolvedTypes
    } yield TableColumn(name, resolvedType)

    TableLiteral(columns, contents)
  }

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[TableLiteral] = {
    if (understands(ts)) Option(TableLiteral(toCells(ts))) else None
  }

  override def parseQueryable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[TableLiteral] = {
    if (understands(ts)) Option(TableLiteral(toCells(ts))) else None
  }

  override def help: List[HelpDoc] = Nil

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts.isTable

  private def toCells(ts: TokenStream): List[List[String]] = {
    var list: List[List[String]] = Nil
    while (ts.hasNext && ts.isTable) {
      ts.next() match {
        case t: TableToken if t.value.isEmpty =>
        case t: TableToken => list = t.value :: list
        case _ =>
      }
    }
    list.reverse
  }

}