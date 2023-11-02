package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.HelpDoc.{CATEGORY_ASYNC_REACTIVE, PARADIGM_REACTIVE}
import com.lollypop.language.models.{Instruction, Invokable}
import com.lollypop.language.{ExpressionParser, HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.{LollypopVM, Scope}
import lollypop.io.IOCost

import scala.concurrent.Future

/**
 * Represents an asynchronous operation
 * @param code the [[Instruction operation]] to execute
 */
case class Async(code: Instruction) extends RuntimeExpression with Invokable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Future[Any]) = {
    import scala.concurrent.ExecutionContext.Implicits.global
    (scope, IOCost.empty, Future(LollypopVM.execute(scope, code)._3))
  }

  override def toSQL: String = s"async ${code.toSQL}"

}

object Async extends ExpressionParser with InvokableParser {
  private val templateCard = "async %N:code"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "async",
    category = CATEGORY_ASYNC_REACTIVE,
    paradigm = PARADIGM_REACTIVE,
    syntax = templateCard,
    description = "Asynchronously executes an instruction",
    example = """async { OS.listFiles("./app") }"""
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Async] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, templateCard)
      params.instructions.get("code").map(Async.apply)
    } else None
  }

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Async] = {
    parseExpression(ts)
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "async"

}
