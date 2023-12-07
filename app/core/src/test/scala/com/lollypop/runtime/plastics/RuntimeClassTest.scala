package com.lollypop.runtime.plastics

import com.github.ldaniels528.lollypop.FakeModel
import com.lollypop.language.{LollypopUniverse, _}
import com.lollypop.runtime.devices._
import com.lollypop.runtime.instructions.expressions.TableExpression
import com.lollypop.runtime.instructions.queryables.TableRendering
import com.lollypop.runtime.plastics.RuntimeClass._
import com.lollypop.runtime.plastics.RuntimeClass.implicits.{RuntimeClassConstructorSugar, RuntimeClassInstanceSugar}
import com.lollypop.runtime.{DataObject, DynamicClassLoader, LollypopNative, Scope}
import lollypop.io.Encodable
import org.scalatest.funspec.AnyFunSpec

import java.util.UUID
import scala.collection.concurrent.TrieMap

class RuntimeClassTest extends AnyFunSpec {
  implicit val ctx: LollypopUniverse = LollypopUniverse()
  implicit val scope: Scope = ctx.createRootScope()
  implicit val cl: DynamicClassLoader = ctx.classLoader

  describe(classOf[RuntimeClass.type].getSimpleName) {

    it("should detect the presence of a class field") {
      assert(classOf[Integer].containsField(name = "MAX_VALUE"))
    }

    it("should retrieve the value of a class field") {
      assert(classOf[Integer].invokeField(name = "MAX_VALUE") == Integer.MAX_VALUE)
    }

    it("should detect the presence of a instance field") {
      assert(UUID.randomUUID().containsField(name = "leastSigBits"))
    }

    it("should retrieve the value of an instance field") {
      assert(new FakeModel(35).invokeField(name = "value") == 35)
    }

    it("should detect the presence of a parameterless instance method") {
      assert("Hello".containsMethod(name = "length"))
    }

    it("should detect the presence of a instance method with parameters") {
      assert("Hello".containsMethod(name = "substring", params = Seq(0, 0)))
    }

    it("should determine the common ancestor between two classes") {
      assert(getCommonAncestor(classOf[FileRowCollection], classOf[ExternalFilesTableRowCollection]) == classOf[RowCollection])
    }

    it("should retrieve every interface of a class") {
      assert(getInterfaces(classOf[RowCollection]) == List(
        classOf[RecordCollection[_]], classOf[DataObject], classOf[SQLSupport], classOf[TableExpression],
        classOf[TableRendering], classOf[AutoCloseable], classOf[RecordMetadataIO], classOf[RecordStructure],
        classOf[Encodable], classOf[LollypopNative]
      ))
    }

    it("should retrieve every super-class and interface of a class") {
      assert(getSuperClassesAndInterfaces(classOf[RowCollection]) == List(
        classOf[RecordCollection[_]], classOf[DataObject], classOf[SQLSupport], classOf[TableExpression],
        classOf[TableRendering], classOf[AutoCloseable], classOf[RecordMetadataIO], classOf[RecordStructure],
        classOf[Encodable], classOf[LollypopNative]
      ))
    }

    it("should retrieve every super-class of a class") {
      assert(getSuperClasses(classOf[TrieMap[_, _]]) == List(
        classOf[scala.collection.mutable.AbstractMap[_, _]], classOf[scala.collection.AbstractMap[_, _]],
        classOf[scala.collection.AbstractIterable[_]], classOf[java.lang.Object]
      ))
    }

    it("should retrieve the value of a parameterless instance method") {
      assert("Hello".invokeMethod(name = "length") == 5)
    }

    it("should retrieve the value of an instance method") {
      assert("Hello".invokeMethod(name = "substring", params = Seq(0.v, 4.v)) == "Hell")
    }

    it("should retrieve the value of an instance method with multiple alternatives") {
      assert("Hello".invokeMethod(name = "lastIndexOf", params = Seq("l".v)) == 3)
    }

    it("should instantiate a constructor with multiple alternatives") {
      assert(classOf[String].instantiate("it works!".getBytes()) == "it works!")
    }

    it("should decode the modifiers of a field") {
      assert(classOf[String].findField(name = "value")
        .toList.flatMap(f => decodeModifiers(f.getModifiers)) == List("private", "final"))
    }

    it("should decode the modifiers of a method") {
      assert("Hello".findMethod(name = "lastIndexOf", params = Seq("l"))
        .toList.flatMap(m => decodeModifiers(m.getModifiers)) == List("public"))
    }

    it("should dynamically load classes") {
      implicit val scope: Scope = Scope()
      val dependency = "com.google.api-client:google-api-client:2.0.0"
      val files = RuntimeClass.downloadDependencies(dependency)
      files.zipWithIndex.foreach { case (f, n) => info(s"${n + 1}. ${f.getName}") }
      RuntimeClass.loadJarFiles(files)
      val _class = RuntimeClass.getClassByName("com.google.api.client.googleapis.notifications.TypedNotification")
      assert(_class.getSimpleName == "TypedNotification")
    }

    it("should retrieve Scala objects by name") {
      implicit val scope: Scope = Scope()
      val function1 = RuntimeClass.getObjectByName("scala.Function1")(scope.getUniverse.classLoader)
      assert(function1 == Function1)
    }

  }

}
