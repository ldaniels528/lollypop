package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.CATEGORY_SYSTEM_TOOLS
import com.lollypop.language.models.{ContainerInstruction, Expression, Instruction, Invokable}
import com.lollypop.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.instructions.RuntimeInstruction
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

/**
 * This synchronization is implemented in Java with a concept called monitors or locks.
 * @param value the value to synchronize
 * @param code  the synchronization execution code
 */
case class Synchronized(value: Expression, code: Instruction)
  extends Invokable with RuntimeInstruction with ContainerInstruction {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val (sa, ca, lock) = value.execute(scope)
    (sa, ca, lock.asInstanceOf[AnyRef].synchronized {
      code.execute(scope)
    })
  }

  override def toSQL: String = Seq("synchronized", value.toSQL, code.toSQL).mkString(" ")
}

object Synchronized extends InvokableParser {
  val templateCard: String = "synchronized %e:value %i:code"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "synchronized",
    category = CATEGORY_SYSTEM_TOOLS,
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

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Synchronized] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, templateCard)
      Option(Synchronized(value = params.expressions("value"), code = params.instructions("code")))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "synchronized"
}