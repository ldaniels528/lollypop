package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.HelpDoc.{CATEGORY_JVM_REFLECTION, PARADIGM_FUNCTIONAL}
import com.lollypop.language._
import com.lollypop.language.models.{Atom, Expression}
import com.lollypop.runtime.instructions.functions.{AnonymousFunction, FunctionArguments}
import com.lollypop.runtime.instructions.jvm.DeclareClass
import com.lollypop.runtime.plastics.RuntimeClass
import com.lollypop.runtime.plastics.RuntimeClass.implicits.RuntimeClassNameAtomConstructorSugar
import com.lollypop.runtime._
import lollypop.io.IOCost

/**
 * Represents a new instance expression
 * @param typeName the instance type name (e.g. "java.util.Date")
 * @param args     the instantiation [[Expression arguments]]
 * @example {{{
 * new `java.util.Date`(1631508164812)
 * }}}
 * @example {{{
 * import "java.awt.event.MouseListener"
 * import "java.awt.event.MouseEvent"
 * new MouseListener() {
 *    mouseClicked: (e: MouseEvent) => out <=== "mouseClicked"
 *    mousePressed: (e: MouseEvent) => out <=== "mousePressed"
 *    mouseReleased: (e: MouseEvent) => out <=== "mouseReleased"
 *    mouseEntered: (e: MouseEvent) => out <=== "mouseEntered"
 *    mouseExited: (e: MouseEvent) => out <=== "mouseExited"
 * }
 * }}}
 */
case class New(typeName: Atom, args: Expression, methods: Option[Dictionary] = None) extends RuntimeExpression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    // resolve the type
    val result = scope.resolve(typeName.name) match {
      case Some(_class: DeclareClass) => _class.newInstance(args)
      case _ =>
        args match {
          // handle function arguments
          case FunctionArguments(_args) if methods.nonEmpty =>
            assert(_args.isEmpty, args.dieInterfaceParametersNotSupported())
            methods.map(createAnonymousClass(_args, _)).orNull
          // handle spread operator
          case FunctionArguments(Seq(SpreadOperator(ArrayLiteral(value)))) => typeName.instantiate(value: _*)
          case FunctionArguments(Seq(SpreadOperator(Dictionary(value)))) => typeName.instantiate(value.map(_._2): _*)
          case FunctionArguments(Seq(SpreadOperator(x))) => args.dieIllegalType(x)
          // default handling
          case FunctionArguments(_args) => typeName.instantiate(_args: _*)
        }
    }
    (scope, IOCost.empty, result)
  }

  override def toSQL: String = {
    val fargs = args match {
      case FunctionArguments(_args) => _args
    }
    (s"new ${typeName.toSQL}${fargs.map(_.toSQL).mkString("(", ",", ")")}" :: methods.map(_.toSQL).toList).mkString(" ")
  }

  /**
   * Creates a new anonymous instance from a class, interface or trait.
   * @param classArgs    the class constructor arguments
   * @param classMethods the class methods
   * @param scope        the implicit [[Scope scope]]
   * @return a new anonymous instance
   */
  private def createAnonymousClass(classArgs: List[Expression], classMethods: Dictionary)(implicit scope: Scope): AnyRef = {
    import java.lang.reflect.{InvocationHandler, Method, Proxy}
    val _class = RuntimeClass.getClassByName(typeName.name)
    Proxy.newProxyInstance(_class.getClassLoader, Array(_class), new InvocationHandler {
      private val mapping = Map[String, Expression](classMethods.value: _*)

      override def invoke(proxy: Any, method: Method, methodArgs: Array[AnyRef]): Any = {
        method.getName match {
          case name if mapping.contains(name) =>
            mapping(name) match {
              case af@AnonymousFunction(params, code, origin) =>
                val scope0 = (origin || scope).withArguments(params, methodArgs)
                af.updateScope(scope0)
                code.execute(scope0)._3
              case expr => expr.dieIllegalType()
            }
          case "hashCode" => mapping.hashCode
          case "toString" => toSQL
          case _ => args.die(s"No such method '${method.getName}${methodArgs.mkString("(", ", ", ")")}'")
        }
      }
    })
  }

}

object New extends ExpressionParser {
  val templateCard: String = "new %a:name %e:args ?%d:methods"

  def apply(typeName: String, args: Seq[Expression]): New = New(typeName, TupleLiteral(args.toList))

  override def help: List[HelpDoc] = List(
    HelpDoc(
      name = "new",
      category = CATEGORY_JVM_REFLECTION,
      paradigm = PARADIGM_FUNCTIONAL,
      syntax = templateCard,
      description = "The new operator can be used to instantiate JVM classes.",
      example = "new `java.util.Date`()"
    ), HelpDoc(
      name = "new",
      category = CATEGORY_JVM_REFLECTION,
      paradigm = PARADIGM_FUNCTIONAL,
      syntax = templateCard,
      description = "The new operator can be used to instantiate Lollypop-defined classes.",
      example =
        """|import "java.util.Date"
           |class QStock(symbol: String, exchange: String, lastSale: Double, lastSaleTime: Date)
           |stock = new QStock("AAPL", "NASDAQ", 31.23, new Date())
           |stock.lastSale
           |""".stripMargin
    ), HelpDoc(
      name = "new",
      category = CATEGORY_JVM_REFLECTION,
      paradigm = PARADIGM_FUNCTIONAL,
      syntax = templateCard,
      description = "The new operator can be used to create anonymous objects from interfaces or traits.",
      example =
        """|import "java.awt.event.MouseListener"
           |import "java.awt.event.MouseEvent"
           |new MouseListener() {
           |    mouseClicked: (e: MouseEvent) => stdout <=== "mouseClicked"
           |    mousePressed: (e: MouseEvent) => stdout <=== "mousePressed"
           |    mouseReleased: (e: MouseEvent) => stdout <=== "mouseReleased"
           |    mouseEntered: (e: MouseEvent) => stdout <=== "mouseEntered"
           |    mouseExited: (e: MouseEvent) => stdout <=== "mouseExited"
           |}
           |""".stripMargin
    ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[New] = {
    val params = SQLTemplateParams(ts, templateCard)
    val typeName = params.atoms("name")
    val args = params.expressions("args")
    val methods = params.dictionaries.get("methods")
    Option(New(typeName.name, args, methods))
  }

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "new"

}
