package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.HelpDoc.{CATEGORY_MACHINE_LEARNING, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{HelpDoc, QueryableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.devices.RowCollectionZoo.createTempTable
import com.lollypop.runtime.devices._
import com.lollypop.runtime.instructions.expressions.ArrayLiteral
import com.lollypop.runtime.instructions.functions.DecompositionFunction
import com.lollypop.runtime.instructions.queryables.RuntimeQueryable
import com.lollypop.runtime.{Boolean2Int, LollypopVMAddOns, Scope}
import lollypop.io.IOCost

import scala.collection.mutable

/**
 * Decompose Table function
 * @example {{{
 *  decompose table stocks [
 *     Histogram(lastSale, [0, 1, 5, 10, 20, 100, 250, 1000])
 *  ]
 * }}}
 * @param source          the source [[Expression table or query]]
 * @param transformations the column [[ArrayLiteral transformations]]
 */
case class DecomposeTable(source: Expression, transformations: ArrayLiteral)
  extends RuntimeQueryable {

  private case class ColumnExpansion(column: TableColumn,
                                     newColumns: List[TableColumn],
                                     transforms: List[DecompositionFunction])

  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    val (sa, ca, src) = source.search(scope)
    val transform_mappings = transformations.value.map {
      case af: DecompositionFunction => af.getColumnName -> af
      case z => dieIllegalType(z)
    }
    val expansions = produceColumnExpansions(transform_mappings, src)
    val destColumns = expansions.flatMap(_.newColumns)
    val dest = createTempTable(destColumns)
    val cc = populateTable(expansions, src, dest)
    (sa, ca ++ cc, dest)
  }

  override def toSQL: String = s"decompose table ${source.toSQL} ${transformations.toSQL}"

  private def populateTable(expansions: List[ColumnExpansion],
                            src: RowCollection,
                            dest: RowCollection)(implicit scope: Scope): IOCost = {
    val destColumns = dest.columns
    var cost = IOCost()
    src.foreach { srcRow =>
      // produce the destination fields
      val destFields = (expansions zip srcRow.fields) flatMap { case (expansion, srcField) =>
        produceFields(expansion, srcField)
      }
      // produce the destination row
      val destRow = Row(srcRow.id, srcRow.metadata, destColumns, destFields)
      cost ++= dest.insert(destRow)
    }
    cost
  }

  private def produceColumnExpansions(transforms: List[(String, DecompositionFunction)],
                                      rc: RowCollection): List[ColumnExpansion] = {
    val expansions = rc.columns.foldLeft[List[ColumnExpansion]](Nil) { (agg, column) =>
      val myTransforms = transforms.collect {
        case (name, transform) if column.name == name => transform
      }
      val newColumns = column.`type` match {
        case EnumType(values) => produceEnumColumns(values)
        case Float32Type | Float64Type | Int8Type | Int16Type | Int32Type | Int64Type =>
          List(TableColumn(name = column.name, `type` = Int16Type))
        //case t if t.name == StringType.name => scanStringColumn(rc, idx)
        case _ => List(column)
      }
      ColumnExpansion(column, newColumns, myTransforms) :: agg
    }
    expansions.reverse
  }

  private def produceColumnName(value: String): String = {
    // replace non-identifier characters
    val name0 = value.toLowerCase().map {
      case c if c >= 'A' && c <= 'z' => c
      case _ => '_'
    }
    // column names should be is_value (e.g. "is_nyse")
    val name1 = name0 match {
      case s if s.startsWith("is_") => s
      case s => s"is_$s"
    }
    name1
  }

  private def produceEnumColumns(uniqueValues: Seq[String]): List[TableColumn] = {
    val columns = uniqueValues.toList.map { value =>
      val name = produceColumnName(value)
      TableColumn(name = name, `type` = Int16Type)
    }
    columns.sortBy(_.name)
  }

  private def produceFields(expansion: ColumnExpansion,
                            srcField: Field)(implicit scope: Scope): List[Field] = {
    expansion.newColumns.map { column =>
      val value = srcField.value match {
        case Some(value: String) if expansion.column.`type`.name == EnumType.name =>
          val name = produceColumnName(value)
          Some((column.name == name).toInt)
        case Some(value) => produceExpressionOrNah(expansion, value)
        case None => None
      }
      Field(name = column.name, metadata = FieldMetadata(), value)
    }
  }

  private def produceExpressionOrNah(expansion: ColumnExpansion, value: Any)(implicit scope: Scope): Option[Any] = {
    expansion.transforms match {
      case Nil => Some(value)
      case transforms =>
        transforms.foldLeft[Option[Any]](Some(value)) { (value_?, tx) =>
          val myScope = scope.withVariable(tx.getColumnName, value_?.orNull)
          Option(tx.execute(myScope)._3)
        }
    }
  }

  private def scanStringColumn(rc: RowCollection, columnID: Int): List[TableColumn] = {
    // gather all unique values for the column
    val uniqueValues = scanUniqueValues(rc, columnID)

    // create new columns from each unique value
    produceEnumColumns(uniqueValues.toList)
  }

  private def scanUniqueValues(rc: RowCollection, columnID: Int): mutable.HashSet[String] = {
    val uniqueValues = mutable.HashSet[String]()
    rc.foreach { row =>
      val field = row.getField(columnID)
      field.value match {
        case Some(value: String) => uniqueValues.add(value)
        case _ =>
      }
    }
    uniqueValues
  }

}

/**
 * Decompose Table Companion
 */
object DecomposeTable extends QueryableParser {
  val templateCard: String = "decompose table %q:source %e:transformation"

  /**
   * Provides help for instruction
   * @return the [[HelpDoc help-doc]]
   */
  override def help: List[HelpDoc] = List(HelpDoc(
    name = "decompose",
    category = CATEGORY_MACHINE_LEARNING,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Creates the decomposition of a table",
    example =
      """|declare table stocks(
         |  symbol: String(8),
         |  exchange: Enum(AMEX, NASDAQ, NYSE, OTC_BB, OTHER_OTC),
         |  last_sale: Double
         |)
         |insert into @stocks
         |  |--------------------------------|
         |  | symbol | exchange  | last_sale |
         |  |--------------------------------|
         |  | ABC    | OTC_BB    |      0.12 |
         |  | JET    | NASDAQ    |     41.26 |
         |  | APAC   | NYSE      |    116.24 |
         |  | XYZ    | AMEX      |     31.95 |
         |  | JUNK   | OTHER_OTC |     97.61 |
         |  |--------------------------------|
         |decompose table @stocks [
         |   Histogram(last_sale, [0, 1, 5, 10, 20, 100])
         |]
         |""".stripMargin,
    isExperimental = true
  ))

  override def parseQueryable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[DecomposeTable] = {
    if (!understands(ts)) None
    else {
      val params = SQLTemplateParams(ts, templateCard)
      val source = params.queryables("source")
      val transformation = params.expressions("transformation") match {
        case lit: ArrayLiteral => lit
        case z => ts.dieIllegalType(z)
      }
      Some(DecomposeTable(source, transformation))
    }
  }

  /**
   * Indicates whether the next token in the stream cam be parsed
   * @param ts       the [[TokenStream token stream]]
   * @param compiler the [[SQLCompiler compiler]]
   * @return true, if the next token in the stream is parseable
   */
  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = {
    ts is "decompose"
  }

}