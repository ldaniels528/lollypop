package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.HelpDoc.{CATEGORY_DATAFRAMES_INFRA, PARADIGM_DECLARATIVE}
import com.lollypop.language.{HelpDoc, IfNotExists, ModifiableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.DatabaseManagementSystem.createUniqueIndex
import com.lollypop.runtime.{DatabaseObjectRef, Scope}
import lollypop.io.IOCost

/**
 * create unique index statement
 * @param ref         the host [[DatabaseObjectRef host table reference]]
 * @param ifNotExists if true, the operation will not fail when the entity exists
 * @author lawrence.daniels@gmail.com
 */
case class CreateUniqueIndex(ref: DatabaseObjectRef, ifNotExists: Boolean) extends RuntimeModifiable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    val cost = IOCost(created = 1) ++ createUniqueIndex(ref.toNS, ifNotExists)
    (scope, cost, cost)
  }

  override def toSQL: String = {
    ("create unique index" :: (if (ifNotExists) List("if not exists") else Nil) ::: ref.toSQL :: Nil).mkString(" ")
  }

}

object CreateUniqueIndex extends ModifiableParser with IfNotExists {
  private val template = "create unique index ?%IFNE:exists %L:table"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "create unique index",
    category = CATEGORY_DATAFRAMES_INFRA,
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

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[CreateUniqueIndex] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, template)
      Some(CreateUniqueIndex(
        ref = params.locations("table"),
        ifNotExists = params.indicators.get("exists").contains(true)))
    } else None
  }

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "create unique index"

}

