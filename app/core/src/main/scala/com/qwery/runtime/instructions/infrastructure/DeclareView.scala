package com.qwery.runtime.instructions.infrastructure

import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAMES_INFRA, PARADIGM_DECLARATIVE}
import com.qwery.language.models.{Atom, View}
import com.qwery.language.{HelpDoc, IfNotExists, ModifiableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.DatabaseManagementSystem.{createVirtualTable, readVirtualTable}
import com.qwery.runtime.Scope
import com.qwery.runtime.devices.RowCollectionZoo.createTempNS
import com.qwery.util.OptionHelper.OptionEnrichment
import qwery.io.IOCost

import scala.collection.mutable

/**
 * declare view statement
 * @param ref         the [[Atom persistent object reference]]
 * @param view        the given [[View view]]
 * @param ifNotExists if true, the operation will not fail when the entity exists
 * @example
 * {{{
 * declare view OilAndGas
 * as
 * select Symbol, Name, Sector, Industry, `Summary Quote`
 * from Customers
 * where Industry = 'Oil/Gas Transmission'
 * order by Symbol desc
 * }}}
 * @author lawrence.daniels@gmail.com
 */
case class DeclareView(ref: Atom, view: View, ifNotExists: Boolean) extends RuntimeModifiable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    val ns = createTempNS()
    val cost0 = createVirtualTable(ns, view, ifNotExists)
    val vtbl = readVirtualTable(ns)
    (scope.withVariable(ref.name, vtbl), cost0, cost0)
  }

  override def toSQL: String = {
    val sb = new mutable.StringBuilder("declare view ")
    if (ifNotExists) sb.append("if not exists ")
    sb.append(s"${ref.toSQL} ")
    sb.append(s":= ${view.query.toSQL}")
    sb.toString()
  }

}

object DeclareView extends ModifiableParser with IfNotExists {
  val template = "declare view ?%IFNE:exists %a:ref %C(_|:=|as) %Q:query"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "declare view",
    category = CATEGORY_DATAFRAMES_INFRA,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Creates a view",
    example =
      """|declare table Students (name: String(64), grade: Char, ratio: Double) containing (
         ||----------------------------------|
         || name            | grade | ratio  |
         ||----------------------------------|
         || John Wayne      | D     | 0.6042 |
         || Carry Grant     | B     | 0.8908 |
         || Doris Day       | A     | 0.9936 |
         || Audrey Hepburn  | A     | 0.9161 |
         || Gretta Garbeaux | C     | 0.7656 |
         ||----------------------------------|
         |)
         |declare view A_Students as select * from Students where ratio >= 0.9
         |A_Students
         |""".stripMargin
  ))

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): DeclareView = {
    val params = SQLTemplateParams(ts, template)
    DeclareView(ref = params.atoms("ref"),
      View(query = params.queryables.get("query") || ts.dieExpectedQueryable()),
      ifNotExists = params.indicators.get("exists").contains(true))
  }

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "declare view"

}
