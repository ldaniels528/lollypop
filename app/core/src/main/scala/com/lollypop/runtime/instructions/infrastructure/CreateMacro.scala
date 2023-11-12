package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.implicits.MagicImplicits
import com.lollypop.language.HelpDoc.{CATEGORY_CONTROL_FLOW, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.runtime.DatabaseManagementSystem.createMACRO
import com.lollypop.runtime.instructions.MacroLanguageParser
import com.lollypop.runtime.{DatabaseObjectRef, Scope}
import lollypop.io.IOCost

/**
 * create macro statement
 * @param ref         the [[DatabaseObjectRef object reference]]
 * @param `macro`     the given [[Macro Macro function]]
 * @param ifNotExists if true, the operation will not fail when the entity exists
 * @author lawrence.daniels@gmail.com
 */
case class CreateMacro(ref: DatabaseObjectRef, `macro`: Macro, ifNotExists: Boolean)
  extends RuntimeModifiable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    MacroLanguageParser.registerMacro(`macro`)
    createMACRO(ref.toNS, `macro`, ifNotExists) ~> { cost => (scope, cost, cost) }
  }

  override def toSQL: String = {
    ("create macro" :: (if (ifNotExists) List("if not exists") else Nil) :::
      ref.toSQL :: ":=" :: s"\"${`macro`.template}\"" :: `macro`.code.toSQL :: Nil).mkString(" ")
  }

}

object CreateMacro extends ModifiableParser with SQLLanguageParser {
  private[infrastructure] val template = "create macro ?%IFNE:exists %L:name := %e:template %i:code"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "create macro",
    category = CATEGORY_CONTROL_FLOW,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Creates a persistent macro",
    example =
      """|namespace "temp.examples"
         |create macro if not exists n_tickers := "tickers %e:qty" {
         |  [1 to qty].map(_ => {
         |      exchange = ['AMEX', 'NASDAQ', 'NYSE', 'OTCBB', 'OTHER_OTC'][Random.nextInt(5)]
         |      is_otc = exchange.startsWith("OT")
         |      lastSaleLimit = switch exchange case "OTCBB" then 5.0 case "OTHER_OTC" then 1.0 case _ then 100.0
         |      lastSale = scaleTo(lastSaleLimit * Random.nextDouble(1.0), 4)
         |      lastSaleTime = DateTime(DateTime() - Duration(1000 * 60 * Random.nextDouble(1.0)))
         |      symbol = Random.nextString(['A' to 'Z'], iff(exchange.startsWith("OT"), Random.nextInt(2) + 4, Random.nextInt(4) + 2))
         |      select lastSaleTime, lastSale, exchange, symbol
         |  }).toTable()
         |}
         |
         |tickers 5
         |""".stripMargin
  ))

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[CreateMacro] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, template)
      Some(CreateMacro(
        ref = params.locations("name"),
        ifNotExists = params.indicators.get("exists").contains(true),
        `macro` = Macro.parseMacro(params)))
    } else None
  }

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "create macro"

}