package com.qwery.runtime.instructions.queryables

import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAMES_IO, PARADIGM_DECLARATIVE}
import com.qwery.language.models.{Instruction, Queryable}
import com.qwery.language.{HelpDoc, QueryableParser, SQLCompiler, SQLTemplateParams, TokenStream, dieNoResultSet}
import com.qwery.runtime.QweryVM.convertToTable
import com.qwery.runtime.devices.RowCollection
import com.qwery.runtime.{QweryVM, Scope}
import qwery.io.IOCost

import scala.annotation.tailrec

case class From(source: Instruction) extends RuntimeQueryable {
  override def search()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    @tailrec
    def recurse(value: Any): RowCollection = value match {
      case None => dieNoResultSet()
      case Some(v) => recurse(v)
      case a: Array[_] => convertToTable(a)
      case r: RowCollection => r
      case s: Scope => s.toRowCollection
      case t: TableRendering => t.toTable
      case x => dieIllegalType(x)
    }

    val (_, cost, result) = QweryVM.execute(scope, source)
    (scope, cost, recurse(result))
  }

  override def toSQL: String = Seq("from", source.wrapSQL).mkString(" ")

}

object From extends QueryableParser {
  val template = "from %i:source"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "from",
    category = CATEGORY_DATAFRAMES_IO,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Retrieves rows from a datasource",
    example = """from [{ item: "Apple" }, { item: "Orange" }, { item: "Cherry" }]"""
  ))

  override def parseQueryable(ts: TokenStream)(implicit compiler: SQLCompiler): Queryable = {
    val params = SQLTemplateParams(ts, template)
    From(source = params.instructions("source"))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "from"
}