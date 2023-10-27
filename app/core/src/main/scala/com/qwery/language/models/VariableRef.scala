package com.qwery.language.models

import com.qwery.language.HelpDoc.{CATEGORY_SCOPE_SESSION, PARADIGM_DECLARATIVE}
import com.qwery.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream}

/**
 * Represents a reference to a variable
 * @author lawrence.daniels@gmail.com
 */
trait VariableRef extends IdentifierRef

object VariableRef extends ExpressionParser {

  def unapply(ref: VariableRef): Option[String] = Some(ref.name)

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "@",
    category = CATEGORY_SCOPE_SESSION,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = "@`variable`",
    description = "used to disambiguate a variable from a field or other identifiers",
    example =
      """|x = 1
         |@x
         |""".stripMargin
  ), HelpDoc(
    name = "@@",
    category = CATEGORY_SCOPE_SESSION,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = "@@`variable`",
    description = "used to disambiguate a table variable from a field or other identifiers",
    example =
      """|r = select value: 1
         |@@r
         |""".stripMargin
  ))

  override def parseExpression(stream: TokenStream)(implicit compiler: SQLCompiler): Option[Expression] = {
    stream match {
      // is it a variable? (e.g. @totalCost)
      case ts if ts is "@" => compiler.nextVariableReference(ts)
      case ts if ts nextIf "@@" => Option(compiler.nextTableVariable(ts))
      case _ => None
    }
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = Seq("@", "@@").exists(ts is _)

}
