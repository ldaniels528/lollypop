package com.lollypop.runtime.plastics

import com.lollypop.language.{LifestyleExpressions, LifestyleExpressionsAny}
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.jvm.DeclareClass
import com.lollypop.runtime.plastics.Plastic.{hasOne, isEmpty}
import com.lollypop.util.StringRenderHelper.StringRenderer

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import scala.reflect.{ClassTag, classTag}

/**
 * Approximates a Scala Product instance
 * @see [[Product]]
 */
trait PlasticProduct extends Plastic with Product {

  def productToMap: Map[String, Any]

}

/**
 * Plastic Product Utilities
 */
object PlasticProduct {

  /**
   * Creates a new [[Product]]-like object instance
   * @param declaredClass the [[DeclareClass declared class]]
   * @param fieldNames    the field names
   * @param fieldValues   the field values
   * @param scope0        the initial [[Scope scope]]
   * @tparam A the type of the object to be created
   * @return the [[PlasticProduct plastic product object]]
   */
  def apply[A <: PlasticProduct : ClassTag](declaredClass: DeclareClass, fieldNames: Seq[String], fieldValues: Seq[Any])(implicit scope0: Scope): A = {
    val interfaces = Array(classTag[A].runtimeClass)
    newInstance(declaredClass, fieldNames, fieldValues, interfaces)
  }

  /**
   * Approximates a JVM object instance
   * @param declaredClass the [[DeclareClass declared class]]
   * @param fieldNames    the field names
   * @param fieldValues   the field values
   * @param interfaces    the collection of interfaces
   * @param scope0        the initial [[Scope scope]]
   * @tparam A the type of the object to be created
   * @return the [[Plastic plastic object]]
   */
  def newInstance[A <: Plastic](declaredClass: DeclareClass, fieldNames: Seq[String], fieldValues: Seq[Any], interfaces: Seq[Class[_]])(implicit scope0: Scope): A = {
    import declaredClass.className.{name => className}
    val fields = fieldNames zip fieldValues
    val componentType: Class[_] = null
    Proxy.newProxyInstance(scope0.getUniverse.classLoader, interfaces.toArray, new InvocationHandler {
      var scope: Scope = Scope()
      val elementName: (Seq[_], Int) => String = (args, index) =>
        if (!isEmpty(args) && index < args.size) fieldNames(index) else null

      override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): Any = {
        method.getName match {
          // Product.*
          case name if isEmpty(args) && fieldNames.contains(name) => fieldValues(fieldNames.indexOf(name))
          case "clone" if isEmpty(args) => newInstance(declaredClass, fieldNames, fieldValues, interfaces)(scope)
          case "copy" => newInstance(declaredClass, fieldNames, fieldValues, interfaces)(scope)
          case "equals" if hasOne(args) => args.headOption.exists(_.hashCode() == proxy.hashCode())
          case "hashCode" if isEmpty(args) => fieldValues.hashCode()
          case "isArray" if isEmpty(args) => componentType != null
          case "productArity" if isEmpty(args) => fields.size
          case "productElementName" => elementName(args, 0)
          case "productElementNames" if isEmpty(args) => fieldNames.iterator
          case "productIterator" if isEmpty(args) => fieldValues.iterator
          case "productPrefix" if isEmpty(args) => className
          case "productToMap" if isEmpty(args) => Map(fieldNames zip fieldValues: _*)
          case "toString" if isEmpty(args) => s"$className${fieldValues.map(_.renderAsJson).mkString("(", ", ", ")")}"
          // _.$name(args)
          case name =>
            val (s, _, r) = name.fx(args.map(_.v): _*).execute()(scope)
            scope = s
            r
        }
      }
    }).asInstanceOf[A]
  }

}