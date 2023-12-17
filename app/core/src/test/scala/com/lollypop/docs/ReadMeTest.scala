package com.lollypop.docs

import com.lollypop.language.HelpDoc
import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime._

import java.io.{File, FileWriter, PrintWriter}
import scala.io.Source
import scala.language.reflectiveCalls

/**
 * Lollypop ReadMe document generator (README.md)
 */
class ReadMeTest extends DocumentGeneratorFunSpec {
  private val mdFile = baseDirectory / "README.md"

  describe("README.md") {

    it("should generate a file containing examples for all documented instructions") {
      implicit val scope: Scope = ctx.createRootScope()
      val featuredExamples: List[HelpDoc] = ctx.helpDocs.filter(_.featureTitle.nonEmpty).sortBy(_.featureTitle)
      new PrintWriter(new FileWriter(mdFile)) use { out =>
        out.println(
          s"""|Lollypop v$version
              |============
              |
              |## Table of Contents
              |* <a href="#Introduction">Introduction</a>
              |* <a href="#Project_Status">Project Status</a>
              |* <a href="#Getting_Started">Getting Started</a>
              |* <a href="#Shell_Scripting">As a Shell-Scripting Language Alternative</a>
              |* <a href="#Basic_Examples">Basic Features</a>
              |""".stripMargin.trim)

        // include the basic features in the ToC
        for {
          title <- featuredExamples.flatMap(_.featureTitle)
        } out.println(s"""  * <a href="#${toAnchor(title)}">$title</a>""")

        // include the instruction examples in the ToC
        out.println("""* <a href="#Examples">Featured Examples By Category</a>""")
        val categoryMappings = ctx.helpDocs.filter(_.featureTitle.isEmpty).groupBy(_.category).toList.sortBy(_._1.toLowerCase())
        for {
          (category, instructions) <- categoryMappings
        } out.println(s"""  * <a href="#${toAnchor(category)}">$category</a> (${instructions.size})""")

        // include the introduction and project status
        introduction(out)
        projectStatus(out)
        gettingStarted(out)

        // include shell-scripting.md
        out.println(
          """|<a name="Shell_Scripting"></a>
             |## As a Shell-Scripting Language Alternative
             |""".stripMargin)
        include(out, docsDirectory / "shell-scripting.md")

        // include featured examples
        out.println(
          """|<a name="Basic_Examples"></a>
             |## Basic Features
             |""".stripMargin)
        for {doc <- featuredExamples} invoke(out, doc)

        // include examples by category
        out.println(
          """|<a name="Examples"></a>
             |""".stripMargin)
        for {
          (category, instructions) <- categoryMappings
        } processCategory(out, category, instructions)
      }
      assert(mdFile.exists())
    }

  }

  private def processCategory(out: PrintWriter, category: String, instructions: Seq[HelpDoc]): Unit = {
    val superScript: Int => String = {
      val array = Array("¹", "²", "³", "⁴", "⁵", "⁶", "⁷", "⁸", "⁹", "¹⁰")
      n => if (n < array.length) array(n) else ""
    }
    out.println(
      s"""|<a name="${toAnchor(category)}"></a>
          |## $category Examples
          |<hr>
          |""".stripMargin)
    val instructionsByName = instructions.groupBy(_.name).toList.sortBy(_._1.toLowerCase)
    instructionsByName foreach { case (name, helps) =>
      out.println(s"""<a name="${toAnchor(name)}"></a>""")
      helps.zipWithIndex.foreach { case (help, n) =>
        val nth = if (helps.size > 1) superScript(n) else ""

        // execute and produce results
        implicit val scope: Scope = ctx.createRootScope()
        invoke(out, help, nth)
      }
    }
  }

  private def include(out: PrintWriter, file: File): Unit = {
    Source.fromFile(file).use(_.getLines().foreach(out.println))
  }

  private def gettingStarted(out: PrintWriter): Unit = {
    val coreAssembly = s"core-assembly-$version.jar"
    val jdbcAssembly = s"jdbc-driver-assembly-$version.jar"
    out.println(
      s"""|<a name="Getting_Started"></a>
          |## Getting Started
          |### To build Lollypop Core (Client and Server)
          |```bash
          |sbt "project core" clean assembly
          |```
          |The Jar binary should be `./app/core/target/scala-2.13/$coreAssembly`
          |
          |### To build the Lollypop JDBC driver
          |```bash
          |sbt "project jdbc_driver" clean assembly
          |```
          |The Jar binary should be `./app/jdbc-driver/target/scala-2.13/$jdbcAssembly`
          |
          |### Run Lollypop REPL
          |```bash
          |sbt "project core" run
          |```
          |OR
          |```bash
          |java -jar ./app/core/target/scala-2.13/$coreAssembly
          |```
          |""".stripMargin)
  }

  private def introduction(out: PrintWriter): Unit = {
    out.println(
      """|<a name="Introduction"></a>
         |## Introduction
         |Lollypop is a general-purpose programming/scripting language for the JVM.
         |Features include:
         |* Native support for Scala and Java classes, objects and packages.
         |* Native support for JSON (arrays, dictionaries and objects).
         |* Native support for Maven package repositories.
         |* Data-oriented types - Dataframes, BLOB/CLOB and Matrices and Vectors.
         |* Multi-paradigm programming model - declarative/SQL, functional, object-oriented and reactive.
         |""".stripMargin)
  }

  private def projectStatus(out: PrintWriter): Unit = {
    out.println(
      """|<a name="Project_Status"></a>
         |## Project Status
         |
         |Preview &#8212; there are still a number of experimental features to sort out.
         |""".stripMargin)
  }

}