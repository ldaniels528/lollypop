package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.HelpDoc.{CATEGORY_CONCURRENCY, PARADIGM_REACTIVE}
import com.lollypop.language.models.{ContainerInstruction, Expression, Instruction, Invokable}
import com.lollypop.language.{ExpressionParser, HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime._
import com.lollypop.runtime.instructions.RuntimeInstruction
import lollypop.io.IOCost

import scala.concurrent.Future

/**
 * Represents an asynchronous operation
 * @param code the [[Instruction instruction]] to execute
 */
case class Async(code: Instruction)
  extends Expression with Invokable with RuntimeInstruction with ContainerInstruction {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Future[_]) = {
    import scala.concurrent.ExecutionContext.Implicits.global
    code.pull(x => Future(x))(scope)
  }

  override def toSQL: String = s"async ${code.toSQL}"

}

object Async extends ExpressionParser with InvokableParser {
  private val templateCard = "async %N:code"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "async",
    category = CATEGORY_CONCURRENCY,
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
