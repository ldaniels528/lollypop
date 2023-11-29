package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_SYSTEM_TOOLS, PARADIGM_OBJECT_ORIENTED}
import com.lollypop.language.models.{Atom, CodeBlock, Instruction}
import com.lollypop.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.functions.NamedFunction
import com.lollypop.runtime.plastics.{RuntimeClass, VirtualMethod}
import com.lollypop.util.LogUtil
import lollypop.io.IOCost

/**
 * Binds a virtual method to a class
 * @example {{{
 * implicit class `java.lang.String` {
 *     def reverseString(value: String) := {
 *         import "java.lang.StringBuilder"
 *         val src = value.toCharArray()
 *         val dest = new StringBuilder(value.length())
 *         val eol = value.length() - 1
 *         var n = 0
 *         while (n <= eol) {
 *           dest.append(src[eol - n])
 *           n += 1
 *         }
 *         dest.toString()
 *     }
 * }
 *
 * "Hello World".reverseString()
 * }}}
 */
case class DefineImplicit(className: Atom, methods: Instruction) extends RuntimeInvokable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {

    def expand(instruction: Instruction): List[NamedFunction] = instruction match {
      case CodeBlock(instructions) => instructions.flatMap(expand)
      case Def(nf: NamedFunction) => List(nf)
      case x => LogUtil(this).info(s"methods: $x"); Nil
    }

    // get the class to extend
    val _class = RuntimeClass.getClassByName(className.name)

    // register the extension functions
    expand(methods).foreach { nf =>
      RuntimeClass.registerVirtualMethod(VirtualMethod(_class, nf))
    }
    (scope, IOCost.empty, null)
  }

  override def toSQL: String = List("implicit", "class", className.toSQL, methods.toSQL).mkString(" ")

}

object DefineImplicit extends InvokableParser {
  private val template = "implicit class %a:class %i:methods"

  override def help: List[HelpDoc] = {
    import com.lollypop.util.OptionHelper.implicits.risky._
    List(HelpDoc(
      name = "implicit",
      category = CATEGORY_SYSTEM_TOOLS,
      paradigm = PARADIGM_OBJECT_ORIENTED,
      syntax = template,
      featureTitle = "Implicit Class Declarations",
      description = "Binds a virtual method to a class",
      example =
        """|implicit class `java.lang.String` {
           |    def reverseString(self) := {
           |        import "java.lang.StringBuilder"
           |        val src = self.toCharArray()
           |        val dest = new StringBuilder(self.length())
           |        val eol = self.length() - 1
           |        var n = 0
           |        while (n <= eol) {
           |          dest.append(src[eol - n])
           |          n += 1
           |        }
           |        dest.toString()
           |    }
           |}
           |
           |"Hello World".reverseString()
           |""".stripMargin
    ))
  }

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[DefineImplicit] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, template)
      Some(DefineImplicit(className = params.atoms("class"), methods = params.instructions("methods")))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "implicit class"

}