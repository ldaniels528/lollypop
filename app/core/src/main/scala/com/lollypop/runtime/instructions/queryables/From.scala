package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.HelpDoc.{CATEGORY_DATAFRAMES_IO, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Instruction
import com.lollypop.language.{HelpDoc, QueryableParser, SQLCompiler, SQLTemplateParams, TokenStream, dieNoResultSet}
import com.lollypop.runtime.conversions.TableConversion.convertSeqToTable
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

import scala.annotation.tailrec

case class From(source: Instruction) extends RuntimeQueryable {
  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    @tailrec
    def recurse(value: Any): RowCollection = value match {
      case None => dieNoResultSet()
      case Some(v) => recurse(v)
      case a: Array[_] => convertSeqToTable(a)
      case r: RowCollection => r
      case s: Scope => s.toRowCollection
      case t: TableRendering => t.toTable
      case x => dieIllegalType(x)
    }

    source.execute(scope).map(recurse)
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

  override def parseQueryable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[From] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, template)
      Some(From(source = params.instructions("source")))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "from"
}