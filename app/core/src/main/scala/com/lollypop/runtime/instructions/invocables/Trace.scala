package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_SYSTEM_TOOLS, PARADIGM_FUNCTIONAL}
import com.lollypop.language.models.Instruction
import com.lollypop.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.{LollypopVM, Scope}
import com.lollypop.util.StringRenderHelper.StringRenderer
import lollypop.io.IOCost

case class Trace(instruction: Instruction) extends RuntimeInvokable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val scope1 = scope.withTrace({ (op, scope, result, elapsedTime) =>

      def friendlyType(result: Any): String = Option(result).map(_.getClass.getSimpleName).orNull

      def friendlyValue(result: Any): String = result match {
        case null => "null"
        case aScope: Scope => aScope.getValueReferences.keys.toList.render
        case v => v.renderAsJson
      }

      def opCode(instruction: Instruction): String = {
        s"${instruction.getClass.getSimpleName} ${instruction.toSQL}"
      }

      scope.stdErr.println(f"[$elapsedTime%.6fms] ${opCode(op)} ~> ${friendlyValue(result)} <${friendlyType(result)}>")
    })
    val (s, c, r) = LollypopVM.execute(scope1, instruction)
    (scope, c, r)
  }

  override def toSQL: String = s"trace ${instruction.toSQL}"
}

object Trace extends InvokableParser {
  val templateCard = "trace %i:instruction"

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Trace = {
    val params = SQLTemplateParams(ts, templateCard)
    Trace(params.instructions("instruction"))
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "trace",
    category = CATEGORY_SYSTEM_TOOLS,
    paradigm = PARADIGM_FUNCTIONAL,
    syntax = templateCard,
    description = "Executes an instruction",
    example = "trace set x = 1"
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "trace"
}