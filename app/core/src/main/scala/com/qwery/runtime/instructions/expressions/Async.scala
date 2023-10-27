package com.qwery.runtime.instructions.expressions

import com.qwery.language.HelpDoc.{CATEGORY_ASYNC_REACTIVE, PARADIGM_REACTIVE}
import com.qwery.language.models.{Instruction, Invokable}
import com.qwery.language.{ExpressionParser, HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.{QweryVM, Scope}

import scala.concurrent.Future

/**
 * Represents an asynchronous operation
 * @param code the [[Instruction operation]] to execute
 */
case class Async(code: Instruction) extends RuntimeExpression with Invokable {

  override def evaluate()(implicit scope: Scope): Future[Any] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    Future(QweryVM.execute(scope, code)._3)
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
    val params = SQLTemplateParams(ts, templateCard)
    params.instructions.get("code").map(Async.apply)
  }

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Async = {
    val params = SQLTemplateParams(ts, templateCard)
    Async(code = params.instructions("code"))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "async"

}
