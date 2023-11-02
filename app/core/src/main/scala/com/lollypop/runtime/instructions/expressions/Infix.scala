package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.models.Expression.implicits.RichAliasable
import com.lollypop.language.models._
import com.lollypop.language.{ExpressionChainParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.LollypopVM.implicits.RichScalaAny
import com.lollypop.runtime.devices.QMap
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.lollypop.runtime.plastics.RuntimeClass.implicits.{RuntimeClassExpressionSugar, RuntimeClassInstanceSugar}
import com.lollypop.runtime.{LollypopVM, Scope}
import com.lollypop.util.JSONSupport._
import com.lollypop.util.JVMSupport.NormalizeAny
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost
import spray.json.JsValue

/**
 * Represents an infix expression (e.g. "({{ total: 100 }}).total == 100")
 * @param instance the [[Expression host object]]
 * @param member   the [[Expression field, member or attribute]] being referenced
 */
case class Infix(instance: Expression, member: Expression) extends RuntimeExpression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val result = member match {
      case ds if ds.getAlias.exists(scope.isDataSource) => scope.getDataSourceValue(ds.getNameOrDie, member.getNameOrDie).orNull
      case call: NamedFunctionCall => instance.invokeMember(call)
      case IdentifierRef(name) => getAttribute(instance, name)
      case other => other.dieIllegalType(other)
    }
    (scope, IOCost.empty, result.unwrapOptions)
  }

  override def toSQL: String = Seq(instance, member).map(_.wrapSQL).mkString(".")

  private def getAttribute(expression: Expression, fieldName: String)(implicit scope: Scope): Any = {
    val rawValue = LollypopVM.execute(scope, expression)._3
    rawValue.normalizeJava match {
      case m: QMap[String, _] if m.contains(fieldName) => m.get(fieldName).orNull
      case js: JsValue =>
        js.unwrapJSON match {
          case m: QMap[String, _] if m.contains(fieldName) => m(fieldName)
          case _ => js.invokeField(fieldName)
        }
      case p: Product =>
        Map(p.productElementNames.toSeq zip p.productIterator: _*)
          .getOrElse(fieldName, p.invokeField(fieldName))
      case _ => rawValue.invokeField(fieldName)
    }
  }

}

object Infix extends ExpressionChainParser {

  override def help: List[HelpDoc] = Nil

  override def parseExpressionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Expression] = {
    if (ts nextIf ".") NamedExpression.parseExpression(ts).map(Infix(host, _)) ?? ts.dieExpectedExpression() else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "."

}