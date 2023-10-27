package com.qwery.runtime.instructions.expressions

import com.qwery.language.models.Expression.implicits.RichAliasable
import com.qwery.language.models._
import com.qwery.language.{ExpressionChainParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.QweryVM.implicits.RichScalaAny
import com.qwery.runtime.devices.QMap
import com.qwery.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.qwery.runtime.plastics.RuntimeClass.implicits.{RuntimeClassExpressionSugar, RuntimeClassInstanceSugar}
import com.qwery.runtime.{QweryVM, Scope}
import com.qwery.util.JSONSupport._
import com.qwery.util.JVMSupport.NormalizeAny
import com.qwery.util.OptionHelper.OptionEnrichment
import spray.json.JsValue

/**
 * Represents an infix expression (e.g. "({{ total: 100 }}).total == 100")
 * @param instance the [[Expression host object]]
 * @param member   the [[Expression field, member or attribute]] being referenced
 */
case class Infix(instance: Expression, member: Expression) extends RuntimeExpression {

  override def evaluate()(implicit scope: Scope): Any = {
    val result = member match {
      case ds if ds.getAlias.exists(scope.isDataSource) => scope.getDataSourceValue(ds.getNameOrDie, member.getNameOrDie).orNull
      case call: NamedFunctionCall => instance.invokeMember(call)
      case IdentifierRef(name) => getAttribute(instance, name)
      case other => other.dieIllegalType(other)
    }
    result.unwrapOptions
  }

  override def toSQL: String = Seq(instance, member).map(_.wrapSQL).mkString(".")

  private def getAttribute(expression: Expression, fieldName: String)(implicit scope: Scope): Any = {
    val rawValue = QweryVM.execute(scope, expression)._3
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