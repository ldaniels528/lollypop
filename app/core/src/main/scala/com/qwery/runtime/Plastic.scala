package com.qwery.runtime

import com.qwery.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.qwery.runtime.RuntimeClass.resolveClass
import com.qwery.util.StringRenderHelper.StringRenderer

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import scala.annotation.tailrec
import scala.reflect.{ClassTag, classTag}

/**
 * Plastic polymorphic data object
 */
trait Plastic extends Product

/**
 * Plastic - data object shaping utilities
 */
object Plastic {

  def newInstance(className: String, fieldNames: Seq[String], fieldValues: Seq[Any])(implicit classLoader: ClassLoader): Plastic = {
    newTypedInstance[Plastic](className, fieldNames, fieldValues)
  }

  def newTypedInstance[A <: Product : ClassTag](className: String, fieldNames: Seq[String], fieldValues: Seq[Any])(implicit classLoader: ClassLoader): A = {
    val fields = fieldNames zip fieldValues
    val `class` = classTag[A].runtimeClass
    Proxy.newProxyInstance(classLoader, Array(`class`), new InvocationHandler {
      var scope: Scope = Scope()
      val isEmpty: Seq[_] => Boolean = args => args == null || args.isEmpty
      val elementName: (Seq[_], Int) => String = (args, index) =>
        if (!isEmpty(args) && index < args.size) fieldNames(index) else null

      override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): Any = method.getName match {
        // Product
        case name if isEmpty(args) && fieldNames.contains(name) => fieldValues(fieldNames.indexOf(name))
        case "copy" => newTypedInstance(className, fieldNames, fieldValues)
        case "productArity" if isEmpty(args) => fields.size
        case "productElementName" => elementName(args, 0)
        case "productElementNames" if isEmpty(args) => fieldNames.iterator
        case "productIterator" if isEmpty(args) => fieldValues.iterator
        case "productPrefix" if isEmpty(args) => className
        // Object
        case "clone" if isEmpty(args) => newTypedInstance(className, fieldNames, fieldValues)
        case "equals" => args.headOption.exists(_.hashCode() == proxy.hashCode())
        case "hashCode" if isEmpty(args) => fields.hashCode()
        case "toString" if isEmpty(args) => s"$className${fieldValues.map(_.renderAsJson).mkString("(", ", ", ")")}"
        // myObject.$name(args)
        case name =>
          val (s, _, r) = QweryVM.execute(scope, name.fx(args.map(_.v): _*))
          scope = s
          r
      }
    }).asInstanceOf[A]
  }

  /**
   * Creates a proxy for the given instance making it possible to intercept method calls on the host instance.
   * @param instance the host instance.
   * @param f        the interceptor function
   * @param classTag the [[ClassTag]] of the type
   * @tparam T the type
   * @return the proxy
   */
  def proxyOf[T](instance: T)(f: PartialFunction[(T, Method, Array[AnyRef]), Any])(implicit classTag: ClassTag[T]): T = {
    val _class = classTag.runtimeClass
    Proxy.newProxyInstance(_class.getClassLoader, Array(_class), new InvocationHandler {
      override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): Any = {
        if (f.isDefinedAt((instance, method, args))) f(instance, method, args) else method.invoke(instance, args: _*)
      }
    }).asInstanceOf[T]
  }

  /**
   * Creates and populates a new array via reflection
   * @param values the source values
   * @return the new array
   */
  def seqToArray(values: Seq[Any]): Array[_] = {
    val _class = resolveClass(values.flatMap(Option.apply).map(_.getClass), isNullable = values.contains(null))
    val array = java.lang.reflect.Array.newInstance(_class, values.length)
    values.zipWithIndex foreach { case (value, index) => java.lang.reflect.Array.set(array, index, value) }
    array.asInstanceOf[Array[_]]
  }

  @tailrec def seqToTuple(value: Any): Option[Any] = {
    import com.qwery.util.OptionHelper.implicits.risky._
    value match {
      case array: Array[_] => seqToTuple(array.toSeq)
      case Seq(a, b) => (a, b)
      case Seq(a, b, c) => (a, b, c)
      case Seq(a, b, c, d) => (a, b, c, d)
      case Seq(a, b, c, d, e) => (a, b, c, d, e)
      case Seq(a, b, c, d, e, f) => (a, b, c, d, e, f)
      case Seq(a, b, c, d, e, f, g) => (a, b, c, d, e, f, g)
      case Seq(a, b, c, d, e, f, g, h) => (a, b, c, d, e, f, g, h)
      case Seq(a, b, c, d, e, f, g, h, i) => (a, b, c, d, e, f, g, h, i)
      case Seq(a, b, c, d, e, f, g, h, i, j) => (a, b, c, d, e, f, g, h, i, j)
      case Seq(a, b, c, d, e, f, g, h, i, j, k) => (a, b, c, d, e, f, g, h, i, j, k)
      case Seq(a, b, c, d, e, f, g, h, i, j, k, l) => (a, b, c, d, e, f, g, h, i, j, k, l)
      case Seq(a, b, c, d, e, f, g, h, i, j, k, l, m) => (a, b, c, d, e, f, g, h, i, j, k, l, m)
      case Seq(a, b, c, d, e, f, g, h, i, j, k, l, m, n) => (a, b, c, d, e, f, g, h, i, j, k, l, m, n)
      case Seq(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o) => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)
      case Seq(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p) => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
      case Seq(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q) => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q)
      case Seq(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r) => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r)
      case Seq(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s) => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)
      case Seq(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t) => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
      case Seq(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u) => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u)
      case Seq(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v) => (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v)
      case _ => None
    }
  }

  def tupleToSeq(value: Any): Option[List[Any]] = {
    import com.qwery.util.OptionHelper.implicits.risky._
    value match {
      case (a, b) => List(a, b)
      case (a, b, c) => List(a, b, c)
      case (a, b, c, d) => List(a, b, c, d)
      case (a, b, c, d, e) => List(a, b, c, d, e)
      case (a, b, c, d, e, f) => List(a, b, c, d, e, f)
      case (a, b, c, d, e, f, g) => List(a, b, c, d, e, f, g)
      case (a, b, c, d, e, f, g, h) => List(a, b, c, d, e, f, g, h)
      case (a, b, c, d, e, f, g, h, i) => List(a, b, c, d, e, f, g, h, i)
      case (a, b, c, d, e, f, g, h, i, j) => List(a, b, c, d, e, f, g, h, i, j)
      case (a, b, c, d, e, f, g, h, i, j, k) => List(a, b, c, d, e, f, g, h, i, j, k)
      case (a, b, c, d, e, f, g, h, i, j, k, l) => List(a, b, c, d, e, f, g, h, i, j, k, l)
      case (a, b, c, d, e, f, g, h, i, j, k, l, m) => List(a, b, c, d, e, f, g, h, i, j, k, l, m)
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n) => List(a, b, c, d, e, f, g, h, i, j, k, l, m, n)
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o) => List(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p) => List(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q) => List(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q)
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r) => List(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r)
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s) => List(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t) => List(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u) => List(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u)
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v) => List(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v)
      case _ => None
    }
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
