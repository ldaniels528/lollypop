package com.qwery.runtime.instructions.infrastructure

import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAME, PARADIGM_DECLARATIVE}
import com.qwery.language.{HelpDoc, IfNotExists, ModifiableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.DatabaseManagementSystem.createUniqueIndex
import com.qwery.runtime.{DatabaseObjectRef, Scope}
import qwery.io.IOCost

/**
 * create unique index statement
 * @param ref         the host [[DatabaseObjectRef host table reference]]
 * @param ifNotExists if true, the operation will not fail when the entity exists
 * @author lawrence.daniels@gmail.com
 */
case class CreateUniqueIndex(ref: DatabaseObjectRef, ifNotExists: Boolean) extends RuntimeModifiable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    (scope, IOCost(created = 1) ++ createUniqueIndex(ref.toNS, ifNotExists), true)
  }

  override def toSQL: String = {
    ("create unique index" :: (if (ifNotExists) List("if not exists") else Nil) ::: ref.toSQL :: Nil).mkString(" ")
  }

}

object CreateUniqueIndex extends ModifiableParser with IfNotExists {
  private val template = "create unique index ?%IFNE:exists %L:table"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "create unique index",
    category = CATEGORY_DATAFRAME,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Creates a unique index",
    example =
      """|namespace "temp.examples"
         |drop if exists Stocks
         |create table Stocks (
         |  symbol: String(5),
         |  exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
         |  lastSale: Double,
         |  lastSaleTime: DateTime
         |)
         |create unique index Stocks#symbol
         |""".stripMargin
  ))

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): CreateUniqueIndex = {
    val params = SQLTemplateParams(ts, template)
    CreateUniqueIndex(ref = params.locations("table"), ifNotExists = params.indicators.get("exists").contains(true))
  }

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "create unique index"

}

