package com.qwery.util

import org.objectweb.asm.{ClassReader, ClassVisitor, Opcodes}

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.jar.JarFile
import scala.jdk.CollectionConverters.EnumerationHasAsScala
import scala.util.{Failure, Success, Try}

/**
 * ClassPath Helper
 */
object ClassPathHelper {

  def searchClassPath(): List[String] = {
    searchClassPath(pattern = "")
  }

  def searchClassPath(pattern: String): List[String] = {
    val classpath = System.getProperty("java.class.path")
    val classpathEntries = classpath.split(File.pathSeparatorChar).toList
    classpathEntries.flatMap {
      case path if path.endsWith(".jar") => getClassNamesFromJar(path, pattern)
      case path if Files.isDirectory(Paths.get(path)) => getClassNamesFromDirectory(path, pattern)
      case _ => Nil
    }
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
