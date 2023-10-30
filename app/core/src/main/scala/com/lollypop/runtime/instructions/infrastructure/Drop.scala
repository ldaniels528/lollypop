package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.HelpDoc.{CATEGORY_DATAFRAMES_INFRA, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.runtime.instructions.ReferenceInstruction
import com.lollypop.runtime.{DatabaseManagementSystem, DatabaseObjectRef, Scope}
import lollypop.io.IOCost

/**
 * Deletes a database object
 * @param ref      the [[DatabaseObjectRef database object reference]]
 * @param ifExists indicates an error should not occur if the object does not exist
 */
case class Drop(ref: DatabaseObjectRef, ifExists: Boolean) extends RuntimeModifiable
  with ReferenceInstruction {

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    val cost = DatabaseManagementSystem.dropObject(ref.toNS, ifExists)
    (scope, cost, cost)
  }

  override def toSQL: String = {
    ("drop" :: (if (ifExists) List("if exists") else Nil) ::: ref.toSQL :: Nil).mkString(" ")
  }

}

object Drop extends ModifiableParser with IfExists {
  val templateCard = s"drop ?%IFE:exists %L:ref"

  override def help: List[HelpDoc] = List(
    HelpDoc(
      name = "drop",
      category = CATEGORY_DATAFRAMES_INFRA,
      paradigm = PARADIGM_DECLARATIVE,
      syntax = "drop ?%IFE:exists %L:ref",
      description = "Deletes a database object",
      example =
        """|namespace "temp.examples"
           |drop if exists Stocks
           |create table Stocks (
           |  symbol: String(8),
           |  exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
           |  lastSale: Double,
           |  lastSaleTime: DateTime,
           |  headlines Table ( headline String(128), newsDate DateTime )[100]
           |)
           |drop Stocks
           |""".stripMargin)
  )

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): Drop = {
    val params = SQLTemplateParams(ts, templateCard)
    Drop(ref = params.locations("ref"), ifExists = params.indicators.get("exists").contains(true))
  }

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "drop"

}
