package com.qwery.runtime.plastics

import com.qwery.runtime.Scope
import com.qwery.runtime.instructions.jvm.DeclareClass

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import scala.reflect.ClassTag

/**
 * Approximates a JVM object instance
 * @example {{{
 *    membersOf(classOf("com.qwery.runtime.plastics.Plastic"))
 * }}}
 */
trait Plastic

/**
 * Plastic - data object shaping utilities
 */
object Plastic {
  private[runtime] val isEmpty: Seq[_] => Boolean = args => args == null || args.isEmpty
  private[runtime] val hasOne: Seq[_] => Boolean = args => Option(args).exists(_.size == 1)

  /**
   * Creates a new [[Product]]-like object instance
   * @param declaredClass the [[DeclareClass declared class]]
   * @param fieldNames    the field names
   * @param fieldValues   the field values
   * @param scope         the initial [[Scope scope]]
   * @return the [[PlasticProduct plastic product object]]
   */
  def apply(declaredClass: DeclareClass, fieldNames: Seq[String], fieldValues: Seq[Any])(implicit scope: Scope): PlasticProduct = {
    PlasticProduct[PlasticProduct](declaredClass, fieldNames, fieldValues)
  }

  /**
   * Creates a proxy for the given instance making it possible to intercept method calls on the host instance.
   * @param instance the host instance.
   * @param f        the interceptor function
   * @param classTag the [[ClassTag]] of the type
   * @tparam T the type
   * @return the approximate object
   */
  def wrap[T](instance: T)(f: PartialFunction[(T, Method, Array[AnyRef]), Any])(implicit classTag: ClassTag[T]): T = {
    val _class = classTag.runtimeClass
    Proxy.newProxyInstance(_class.getClassLoader, Array(_class), new InvocationHandler {
      override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): Any = {
        if (f.isDefinedAt((instance, method, args))) f(instance, method, args) else method.invoke(instance, args: _*)
      }
    }).asInstanceOf[T]
  }

  object implicits {
    private val methodDecodeExtras = Seq(
      "$anonfun$" -> "",
      "$this" -> "this")
    private val methodCodes = Seq(
      "$amp" -> "&",
      "$bang" -> "!",
      "$bar" -> "|",
      "$div" -> "/",
      "$eq" -> "=",
      "$greater" -> ">",
      "$less" -> "<",
      "$minus" -> "-",
      "$percent" -> "%",
      "$plus" -> "+",
      "$qmark" -> "?",
      "$times" -> "*",
      "$up" -> "^")

    final implicit class MethodNameConverter(val methodName: String) extends AnyVal {

      def decodeName: String = (methodDecodeExtras ++ methodCodes).foldLeft(methodName) {
        case (name, (code, symbol)) => name.replace(code, symbol)
      }

      def encodeName: String = methodCodes.foldLeft(methodName) {
        case (name, (code, symbol)) => name.replace(symbol, code)
      }

    }

    final implicit class ProductToMap(val product: Product) extends AnyVal {
      @inline
      def toMap: Map[String, Any] = Map(product.productElementNames.zip(product.productIterator).toSeq: _*)
    }

  }

}
