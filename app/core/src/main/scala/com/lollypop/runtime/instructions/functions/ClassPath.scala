package com.lollypop.runtime.instructions.functions

import com.lollypop.language.HelpDoc.CATEGORY_SYSTEM_TOOLS
import com.lollypop.language.models.Expression
import com.lollypop.runtime._
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.instructions.functions.ClassPath.searchClassPath
import lollypop.io.IOCost
import org.objectweb.asm.{ClassReader, ClassVisitor, Opcodes}

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.jar.JarFile
import scala.jdk.CollectionConverters.EnumerationHasAsScala
import scala.util.{Failure, Success, Try}

/**
 * ClassPath - Searches the classpath via a class name pattern.
 * @param pattern the [[Expression expression]] representing the regular expression as the class name pattern.
 * @example {{{
 *   transpose(classPath("akka[.]dispatch[.](.*)D(.*)M(.*)S(.*)"))
 * }}}
 */
case class ClassPath(pattern: Expression) extends ScalarFunctionCall with RuntimeExpression {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Array[String]) = {
    pattern.pullString ~>> searchClassPath
  }
}

/**
 * ClassPath Companion
 */
object ClassPath extends FunctionCallParserE1(
  name = "classPath",
  category = CATEGORY_SYSTEM_TOOLS,
  description =
    """|Searches the classpath via a class name pattern.
       |""".stripMargin,
  example = """transpose(classPath("akka[.]dispatch[.](.*)D(.*)M(.*)S(.*)"))""") {

  def searchClassPath(pattern: String): Array[String] = {
    val classpath = System.getProperty("java.class.path")
    val classpathEntries = classpath.split(File.pathSeparatorChar).toList
    classpathEntries.flatMap {
      case path if path.endsWith(".jar") => getClassNamesFromJar(path, pattern)
      case path if Files.isDirectory(Paths.get(path)) => getClassNamesFromDirectory(path, pattern)
      case _ => Nil
    }.toArray
  }

  private def getClassNamesFromJar(jarPath: String, pattern: String): List[String] = {
    val jarFile = new JarFile(jarPath)
    val entries = jarFile.entries().asScala.toList
    entries.filter(_.getName.endsWith(".class")).flatMap { entry =>
      val className = entry.getName.stripSuffix(".class").replace('/', '.')
      if (matches(className, pattern)) Option(className) else None
    }
  }

  private def getClassNamesFromDirectory(directoryPath: String, pattern: String): List[String] = {
    val directory = new File(directoryPath)
    val classFiles = directory.listFiles.toList.filter(_.isFile).filter(_.getName.endsWith(".class"))
    classFiles.flatMap(file => getClassNameFromFile(file, pattern))
  }

  private def getClassNameFromFile(file: File, pattern: String): Option[String] = {
    Try {
      val classReader = new ClassReader(Files.readAllBytes(file.toPath))
      val classNameVisitor = new ClassNameVisitor
      classReader.accept(classNameVisitor, ClassReader.SKIP_CODE)
      val className = classNameVisitor.className
      if (matches(className, pattern)) Option(className) else None
    } match {
      case Failure(_) => None
      case Success(value) => value
    }
  }

  private def matches(className: String, pattern: String): Boolean = {
    pattern.isEmpty || className.matches(pattern)
  }

  private class ClassNameVisitor extends ClassVisitor(Opcodes.ASM9) {
    var className: String = _

    override def visit(version: Int, access: Int, name: String, signature: String, superName: String, interfaces: Array[String]): Unit = {
      className = name.replace('/', '.')
    }
  }

}
