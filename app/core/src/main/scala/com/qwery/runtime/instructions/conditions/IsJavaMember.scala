package com.qwery.runtime.instructions.conditions

import com.qwery.language.HelpDoc.{CATEGORY_JVM_REFLECTION, PARADIGM_OBJECT_ORIENTED}
import com.qwery.language.models.Expression
import com.qwery.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.Scope
import com.qwery.runtime.instructions.conditions.IsJavaMember.__name
import com.qwery.runtime.plastics.RuntimeClass.implicits.RuntimeClassExpressionSugar

/**
 * Tests the existence of a JVM member (Method/Field)
 * @param instance the [[Expression instance]] to evaluate
 * @param member   the [[Expression member]] whose existence is being tested
 * @example object.?intValue()
 * @example object.?MAX_VALUE
 */
case class IsJavaMember(instance: Expression, member: Expression) extends RuntimeCondition {

  override def isTrue(implicit scope: Scope): Boolean = instance.containsMember(member)

  override def toSQL: String = Seq( instance.wrapSQL, __name, member.wrapSQL).mkString

}

object IsJavaMember extends ExpressionToConditionPostParser {
  private val __name = ".?"

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[IsJavaMember] = {
    if (ts.nextIf(__name)) compiler.nextExpression(ts).map(IsJavaMember(host, _)) else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = __name,
    category = CATEGORY_JVM_REFLECTION,
    paradigm = PARADIGM_OBJECT_ORIENTED,
    syntax = s"`instance`${__name}`methodName`",
    description = "determines whether the method exists within the instance",
    example =
      """|val num = 5
         |num.?MAX_VALUE
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is __name

}