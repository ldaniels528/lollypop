package com.qwery.runtime.instructions.invocables

import com.qwery.language.HelpDoc.CATEGORY_SYNC_IO
import com.qwery.language.models.{Expression, Instruction}
import com.qwery.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.qwery.runtime.{QweryVM, Scope}
import qwery.io.IOCost

/**
 * This synchronization is implemented in Java with a concept called monitors or locks.
 * @param value the value to synchronize
 * @param code  the synchronization execution code
 */
case class Synchronized(value: Expression, code: Instruction) extends RuntimeInvokable {

  override def invoke()(implicit scope: Scope): (Scope, IOCost, Any) = {
    value.asAny match {
      case Some(lock) =>
       lock.asInstanceOf[AnyRef].synchronized {
          QweryVM.execute(scope, code)
        }
      case None => (scope, IOCost.empty, null)
    }
  }

  override def toSQL: String = Seq("synchronized", value.toSQL, code.toSQL).mkString(" ")
}

object Synchronized extends InvokableParser {
  val templateCard: String = "synchronized %e:value %i:code"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "synchronized",
    category = CATEGORY_SYNC_IO,
    syntax = templateCard,
    description = "Synchronizes access to an object; providing an exclusive read/write lock over it",
    example =
      """|bag = { message: null }
         |synchronized(bag) {
         |   bag.message = 'Hello'
         |}
         |bag
         |""".stripMargin
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Synchronized = {
    val params = SQLTemplateParams(ts, templateCard)
    Synchronized(value = params.expressions("value"), code = params.instructions("code"))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "synchronized"
}