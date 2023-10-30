package com.lollypop.runtime.instructions

import com.lollypop.language.models.Expression
import com.lollypop.language.{SQLCompiler, TokenStream}
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

trait VerificationTools { self: AnyFunSpec =>
  private val logger = LoggerFactory.getLogger(getClass)

  def getTestTableName: String = {
    val pcs = self.getClass.getName.split("[.]")
    ("temp" :: pcs.takeRight(2).toList).mkString(".")
  }

  def verify(expectedSQL: String)(implicit compiler: SQLCompiler): Assertion = {
    logger.info(s"expected: $expectedSQL")
    val expected = compiler.compile(expectedSQL)
    val actualSQL = expected.toSQL
    logger.info(s"actual: $actualSQL")
    val actual = compiler.compile(actualSQL)
    assert(actual == expected)
  }

  def verify(expr: String, expect: Expression)(implicit compiler: SQLCompiler): Unit = {
    val actual = compiler.nextExpression(TokenStream(expr))
    info(s"$expr => $actual")
    assert(actual.contains(expect), s"$expr : failed")
  }

}
