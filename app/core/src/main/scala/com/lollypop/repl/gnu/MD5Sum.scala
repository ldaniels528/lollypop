package com.lollypop.repl.gnu

import com.lollypop.language.HelpDoc.{CATEGORY_REPL_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.repl.gnu.MD5Sum.keyword
import com.lollypop.runtime.datatypes.VarBinaryType
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

import java.io.{File, FileInputStream}
import java.security.MessageDigest

/**
 * MD5Sum - Returns an MD5 digest of a byte-encode-able value.
 * @param expression the [[Expression expression]]
 * @example {{{
 *   md5 new `java.io.File`("app/core/src/main/scala/com/lollypop/repl/gnu/MD5Sum.scala")
 * }}}
 */
case class MD5Sum(expression: Expression) extends RuntimeExpression {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Array[Byte]) = {
    expression.execute(scope) map {
      case f: File => new FileInputStream(f).use(_.readAllBytes())
      case z => VarBinaryType.convert(z)
    } map { bytes =>
      val digest = MessageDigest.getInstance("MD5")
      digest.update(bytes)
      digest.digest()
    }
  }

  override def toSQL: String = Seq(keyword, expression.toSQL).mkString(" ")

}

object MD5Sum extends ExpressionParser {
  private val keyword = "md5"
  private val templateCard = s"$keyword %e:path"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Returns an MD5 digest of a file or byte-encode-able value.",
    example =
      """|md5 "BadPassword123"
         |""".stripMargin
  ), HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Returns an MD5 digest of a file or byte-encode-able value.",
    example =
      """|md5 new `java.io.File`("app/core/src/main/scala/com/lollypop/repl/gnu/MD5Sum.scala")
         |""".stripMargin
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[MD5Sum] = {
    if (understands(ts)) {
      val p = SQLTemplateParams(ts, templateCard)
      Some(MD5Sum(p.expressions("path")))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}
