package com.qwery.runtime.instructions.infrastructure

import com.qwery.language.HelpDoc.{CATEGORY_TRANSFORMATION, PARADIGM_DECLARATIVE}
import com.qwery.language._
import com.qwery.language.models.ExternalTable
import com.qwery.runtime.DatabaseManagementSystem.createExternalTable
import com.qwery.runtime.DatabaseObjectConfig.ExternalTableConfig
import com.qwery.runtime.devices.TableColumn
import com.qwery.runtime.devices.TableColumn.implicits.SQLToColumnConversion
import com.qwery.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.qwery.runtime.instructions.infrastructure.CreateExternalTable.ExternalTableDeclaration
import com.qwery.runtime.{DatabaseObjectRef, Scope}
import com.qwery.util.OptionHelper.OptionEnrichment
import qwery.io.IOCost

/**
 * create external table statement
 * @param ref         the [[DatabaseObjectRef persistent object reference]]
 * @param table       the given [[ExternalTable table]]
 * @param ifNotExists if true, the operation will not fail when the entity exists
 * @author lawrence.daniels@gmail.com
 */
case class CreateExternalTable(ref: DatabaseObjectRef, table: ExternalTable, ifNotExists: Boolean) extends RuntimeModifiable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    val cost = createExternalTable(ref.toNS, declaration = parseExternalTableDeclaration())
    (scope, cost, cost)
  }

  override def toSQL: String = {
    ("create external table" :: (if (ifNotExists) List("if not exists") else Nil) ::: ref.toSQL ::
      table.columns.map(c => c.toSQL).mkString("(", ", ", ")") ::
      "containing" :: table.options.toSQL :: Nil).mkString(" ")
  }

  private def parseExternalTableDeclaration()(implicit scope: Scope): ExternalTableDeclaration = {
    val conversions = Seq("\\b" -> "\b", "\\n" -> "\n", "\\r" -> "\r", "\\t" -> "\t")
    val converter: String => String = s => conversions.foldLeft(s) { case (str, (from, to)) => str.replace(from, to) }

    def asBoolean: Any => Boolean = {
      case "true" => true
      case "false" => false
      case b: Boolean => b
      case b: java.lang.Boolean => b
      case x => table.options.dieIllegalType(x)
    }

    def asList: Any => List[String] = {
      case a: Array[String] => a.toList
      case a: Array[_] => a.map(asString).toList
      case s: String => List(s)
      case x => table.options.dieIllegalType(x)
    }

    def asString: Any => String = {
      case c: Char => String.valueOf(c)
      case s: String => converter(s)
      case x => table.options.dieIllegalType(x)
    }

    (for {params <- table.options.asDictionary} yield ExternalTableDeclaration(
      columns = table.columns.map(_.toTableColumn),
      ifNotExists = ifNotExists,
      partitions = params.get("partitions").toList flatMap asList,
      config = ExternalTableConfig(
        fieldDelimiter = params.get("field_delimiter") map asString,
        headers = params.get("headers") map asBoolean,
        format = params.get("format") map asString map (_.toLowerCase),
        lineTerminator = params.get("line_delimiter") map asString,
        location = params.get("location") map asString,
        nullValues = params.get("null_values").toList flatMap asList
      ))) || table.options.dieIllegalType()
  }

}

object CreateExternalTable extends ModifiableParser with IfNotExists {
  val template: String =
    """|create external table ?%IFNE:exists %L:name ( %P:columns ) containing %e:options
       |""".stripMargin

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "create external table",
    category = CATEGORY_TRANSFORMATION,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Creates an external table",
    example =
      """|create external table if not exists customers (
         |  customer_uid: UUID,
         |  name: String,
         |  address: String,
         |  ingestion_date: Long
         |) containing { format: 'json', location: './datasets/customers/json/', null_values: ['n/a'] }
         |""".stripMargin
  ))

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): CreateExternalTable = {
    val params = SQLTemplateParams(ts, template)
    CreateExternalTable(ref = params.locations("name"),
      ExternalTable(
        columns = params.parameters.getOrElse("columns", Nil).map(_.toColumn),
        options = params.expressions("options")),
      ifNotExists = params.indicators.get("exists").contains(true))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "create external table"

  /**
   * External Table Declaration
   * @param columns     the [[TableColumn table columns]]
   * @param partitions  the partition column names
   * @param config      the [[ExternalTableConfig table configuration]]
   * @param ifNotExists if true, the operation will not fail when the table exists
   */
  case class ExternalTableDeclaration(columns: List[TableColumn],
                                      partitions: List[String],
                                      config: ExternalTableConfig,
                                      ifNotExists: Boolean)

}