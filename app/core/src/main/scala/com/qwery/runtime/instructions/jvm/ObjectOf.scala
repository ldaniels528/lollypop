package com.qwery.runtime.instructions.jvm

import com.qwery.language.HelpDoc.{CATEGORY_JVM_REFLECTION, PARADIGM_OBJECT_ORIENTED}
import com.qwery.language.models.Expression
import com.qwery.runtime.instructions.expressions.RuntimeExpression
import com.qwery.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.qwery.runtime.instructions.functions.{FunctionCallParserE1, ScalarFunctionCall}
import com.qwery.runtime.plastics.RuntimeClass.getObjectByName
import com.qwery.runtime.{DynamicClassLoader, Scope}

case class ObjectOf(className: Expression) extends ScalarFunctionCall with RuntimeExpression {

  override def evaluate()(implicit scope: Scope): Any = {
    implicit val classLoader: DynamicClassLoader = scope.getUniverse.classLoader
    className.asString.map(getObjectByName).orNull
  }

}

object ObjectOf extends FunctionCallParserE1(
  name = "objectOf",
  category = CATEGORY_JVM_REFLECTION,
  paradigm = PARADIGM_OBJECT_ORIENTED,
  description = "Returns a Scala object instance by name",
  example = "objectOf('scala.Function1')"
)