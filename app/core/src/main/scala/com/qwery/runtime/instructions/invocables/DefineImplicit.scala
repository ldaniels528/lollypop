package com.qwery.runtime.instructions.invocables

import com.qwery.language.HelpDoc.{CATEGORY_SYSTEMS, PARADIGM_OBJECT_ORIENTED}
import com.qwery.language.models.{Atom, CodeBlock, Instruction}
import com.qwery.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.instructions.functions.NamedFunction
import com.qwery.runtime.plastics.{RuntimeClass, VirtualMethod}
import com.qwery.runtime.Scope
import com.qwery.util.LogUtil
import qwery.io.IOCost

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
      case DefineFunction(nf: NamedFunction) => List(nf)
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

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "implicit",
    category = CATEGORY_SYSTEMS,
    paradigm = PARADIGM_OBJECT_ORIENTED,
    syntax = template,
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

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): DefineImplicit = {
    val params = SQLTemplateParams(ts, template)
    DefineImplicit(className = params.atoms("class"), methods = params.instructions("methods"))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "implicit class"

}