package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.HelpDoc.{CATEGORY_JVM_REFLECTION, PARADIGM_OBJECT_ORIENTED}
import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.conditions.IsJavaMember.keyword
import com.lollypop.runtime.plastics.RuntimeClass.implicits.RuntimeClassExpressionSugar
import lollypop.io.IOCost

/**
 * Tests the existence of a JVM member (Method/Field)
 * @param instance the [[Expression instance]] to evaluate
 * @param member   the [[Expression member]] whose existence is being tested
 * @example object.?intValue()
 * @example object.?MAX_VALUE
 */
case class IsJavaMember(instance: Expression, member: Expression) extends RuntimeCondition {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    (scope, IOCost.empty, instance.containsMember(member))
  }

  override def toSQL: String = Seq(instance.wrapSQL, keyword, member.wrapSQL).mkString

}

object IsJavaMember extends ExpressionToConditionPostParser {
  private val keyword = ".?"

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[IsJavaMember] = {
    if (ts.nextIf(keyword)) compiler.nextExpression(ts).map(IsJavaMember(host, _)) else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_JVM_REFLECTION,
    paradigm = PARADIGM_OBJECT_ORIENTED,
    syntax = s"`instance`$keyword`methodName`",
    description = "determines whether the method exists within the instance",
    example =
      """|val num = 5
         |num.?MAX_VALUE
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}