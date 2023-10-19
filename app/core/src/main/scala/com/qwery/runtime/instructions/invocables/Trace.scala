package com.qwery.runtime.instructions.invocables

import com.qwery.language.HelpDoc.{CATEGORY_SYSTEMS, PARADIGM_FUNCTIONAL}
import com.qwery.language.models.Instruction
import com.qwery.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.{QweryVM, Scope}
import com.qwery.util.StringRenderHelper.StringRenderer
import qwery.io.IOCost

case class Trace(instruction: Instruction) extends RuntimeInvokable {

  override def invoke()(implicit scope: Scope): (Scope, IOCost, Any) = {
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
    val (s, c, r) = QweryVM.execute(scope1, instruction)
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
    category = CATEGORY_SYSTEMS,
    paradigm = PARADIGM_FUNCTIONAL,
    syntax = templateCard,
    description = "Executes an instruction",
    example = "trace set x = 1"
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "trace"
}
