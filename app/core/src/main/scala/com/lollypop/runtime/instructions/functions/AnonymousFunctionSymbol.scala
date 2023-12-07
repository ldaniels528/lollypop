package com.lollypop.runtime.instructions.functions

import com.lollypop.language.HelpDoc.{CATEGORY_SCOPE_SESSION, PARADIGM_FUNCTIONAL}
import com.lollypop.language._
import com.lollypop.language.models.{ColumnType, Expression, Parameter}

/**
 * Anonymous Function Symbol
 * @example {{{
 *  val f = n => n * n
 *  f(5)
 * }}}
 */
trait AnonymousFunctionSymbol extends ExpressionChainParser {

  override def help: List[HelpDoc] = List(
    HelpDoc(
      name = "=>",
      category = CATEGORY_SCOPE_SESSION,
      paradigm = PARADIGM_FUNCTIONAL,
      syntax = "",
      description = "Defines an anonymous function",
      example =
        """|val f = n => n * n
           |f(5)
           |""".stripMargin
    ))

  override def parseExpressionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Expression] = {
    if (ts nextIf "=>") {
      host match {
        case FunctionParameters(params) =>
          Some(AnonymousFunction(params, code = compiler.nextOpCodeOrDie(ts)))
        case ct: ColumnType =>
          val params = Seq(Parameter(name = ct.getNameOrDie, `type` = ct))
          Some(AnonymousFunction(params, code = compiler.nextOpCodeOrDie(ts)))
        case x => ts.dieIllegalType(x)
      }
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "=>"

}
