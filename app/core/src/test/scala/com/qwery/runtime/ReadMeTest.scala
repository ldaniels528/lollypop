package com.qwery.runtime

import com.qwery.AppConstants
import com.qwery.database.server.QweryChartGenerator
import com.qwery.language.models.Instruction
import com.qwery.language.{HelpDoc, QweryUniverse}
import com.qwery.runtime.RuntimeFiles.RecursiveFileList
import com.qwery.runtime.devices.RowCollection
import com.qwery.runtime.devices.RowCollectionZoo.ProductToRowCollection
import com.qwery.runtime.instructions.expressions.GraphResult
import com.qwery.runtime.instructions.queryables.TableRendering
import com.qwery.util.ResourceHelper.AutoClose
import com.qwery.util.StringHelper.StringEnrichment
import com.qwery.util.StringRenderHelper.StringRenderer
import org.scalatest.funspec.AnyFunSpec

import java.io.{File, FileWriter, PrintWriter}
import scala.annotation.tailrec
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.language.reflectiveCalls
import scala.util.Try

/**
 * Qwery README.md generator
 */
class ReadMeTest extends AnyFunSpec {
  private implicit val ctx: QweryUniverse = QweryUniverse(isServerMode = true)
  private val baseDirectory = new File(".") / "docs"
  private val imageDirectory = baseDirectory / "images"
  private val mdFile = new File("./README.md")

  describe(classOf[ReadMeTest].getSimpleName) {

    it("should generate a file containing examples for all documented instructions") {
      new PrintWriter(new FileWriter(mdFile)) use { out =>
        implicit val scope: Scope = ctx.createRootScope()
        out.println(
          s"""|Qwery v${AppConstants.version}
              |============
              |
              |## Table of Contents
              |* <a href="#Introduction">Introduction</a>
              |* <a href="#Project_Status">Project Status</a>
              |* <a href="#Getting_Started">Getting Started</a>
              |* <a href="#Basic_Examples">Basic Features</a>
              |""".stripMargin.trim)

        // include the featured examples in the ToC
        for {
          (title, _) <- featuredExamples
        } out.println(s"""  * <a href="#${toAnchor(title)}">$title</a>""")

        // include the instruction examples in the ToC
        out.println("""* <a href="#Examples">Featured Examples By Category</a>""")
        val categoryMappings = ctx.helpDocs.groupBy(_.category).toList.sortBy(_._1.toLowerCase())
        for {
          (category, instructions) <- categoryMappings
        } out.println(s"""  * <a href="#${toAnchor(category)}">$category</a> (${instructions.size})""")

        // include the introduction and project status
        introduction(out)
        projectStatus(out)
        gettingStarted(out)

        // include featured examples
        out.println(
          """|<a name="Basic_Examples"></a>
             |## Basic Features
             |""".stripMargin)
        for {
          (title, example) <- featuredExamples
        } invoke(out, title, example)

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
    out.println(
      s"""|<a name="${toAnchor(category)}"></a>
          |## $category Examples
          |<hr>
          |""".stripMargin)
    val instructionsByName = instructions.groupBy(_.name).toList.sortBy(_._1.toLowerCase)
    instructionsByName foreach { case (_, helps) =>
      helps.zipWithIndex.foreach { case (help, n) =>
        val nth = if (helps.size > 1) n match {
          case 0 => "¹"
          case 1 => "²"
          case 2 => "³"
          case _ => "°"
        } else ""

        // execute and produce results
        implicit val scope: Scope = ctx.createRootScope()
        invoke(out, help, nth)
      }
    }
  }

  private def invoke(out: PrintWriter, title: String, example: String)(implicit scope: Scope): Unit = {
    // header section
    out.println(
      s"""|<a name="${toAnchor(title)}"></a>
          |### $title
          |""".stripMargin)
    out.println("```sql")
    out.println(example.trim)
    out.println("```")

    // detail section
    for {
      (scope1, _, result1) <- Try(QweryVM.executeSQL(scope, example))
      results <- resolve(result1)
    } {
      out.println("##### Results")
      if (results.contains("<img")) out.println(results)
      else {
        out.println("```sql")
        out.println(results)
        out.println("```")
      }

      showConsoleOutputs(out, scope1)
    }
  }

  private def invoke(out: PrintWriter, help: HelpDoc, nth: String)(implicit scope: Scope): Unit = {
    // header section
    out.println(
      s"""|### ${help.name}$nth (${help.category} &#8212; ${help.paradigm})
          |*Description*: ${help.description.trim}
          |""".stripMargin)
    out.println("```sql")
    out.println(help.example.trim)
    out.println("```")

    // detail section
    for {
      (scope1, _, result1) <- Try(QweryVM.executeSQL(scope, help.example))
      results <- resolve(result1)
    } {
      out.println("##### Results")
      if (results.contains("<img")) out.println(results)
      else {
        out.println("```sql")
        out.println(results)
        out.println("```")
      }

      showConsoleOutputs(out, scope1)
    }
  }

  private def showConsoleOutputs(out: PrintWriter, scope: Scope): Unit = {
    val system = scope.getUniverse.system

    // include STDOUT
    val consoleOut = system.stdOut.asString().trim
    if (consoleOut.nonEmpty) {
      out.println("##### Console Output")
      out.println(
        s"""|```
            |$consoleOut
            |```""".stripMargin)
    }

    // include STDERR
    val consoleErr = system.stdErr.asString().trim
    if (consoleErr.nonEmpty) {
      out.println("##### Console Error")
      out.println(
        s"""|```
            |$consoleErr
            |```""".stripMargin)
    }
  }

  private def introduction(out: PrintWriter): Unit = {
    out.println(
      """|<a name="Introduction"></a>
         |## Introduction
         |Qwery is a general-purpose programming/scripting language for the JVM.
         |Features include:
         |* Native support for Scala and Java classes, objects and packages.
         |* Native support for JSON (arrays, dictionaries and objects).
         |* Native support for Maven package repositories.
         |* Data-oriented types - Dataframes, BLOB/CLOB and Matrices and Vectors.
         |* Multi-paradigm programming model - declarative/SQL, functional and object-oriented.
         |""".stripMargin)
  }

  private def gettingStarted(out: PrintWriter): Unit = {
    val coreAssembly = s"core-assembly-$version.jar"
    val jdbcAssembly = s"jdbc-driver-assembly-$version.jar"
    out.println(
      s"""|<a name="Getting_Started"></a>
          |## Getting Started
          |### To build Qwery Core (Client and Server)
          |```bash
          |sbt "project core" clean assembly
          |```
          |The Jar binary should be `./app/core/target/scala-2.13/$coreAssembly`
          |
          |### To build the Qwery JDBC driver
          |```bash
          |sbt "project jdbc_driver" clean assembly
          |```
          |The Jar binary should be `./app/jdbc-driver/target/scala-2.13/$jdbcAssembly`
          |
          |### Run Query CLI
          |```bash
          |java -jar ./app/core/target/scala-2.13/$coreAssembly
          |```
          |""".stripMargin)
  }

  private def projectStatus(out: PrintWriter): Unit = {
    out.println(
      """|<a name="Project_Status"></a>
         |## Project Status
         |
         |Unstable/Preview &#8212; actively addressing bugs and (re-)implementing missing or broken features.
         |""".stripMargin)
  }

  @tailrec
  private def resolve(outcome: Any)(implicit scope: Scope): Option[String] = {
    outcome match {
      case null | None => None
      case Some(rc: RowCollection) => resolve(rc)
      case dr: GraphResult =>
        val imageFile = QweryChartGenerator.generateFile(imageDirectory, dr)
        Some(
          s"""|<div style="width: 100%">
              |<img src="${imageFile.getPath}">
              |</div>
              |""".stripMargin)
      case fu: Future[_] => resolve(Try(Await.result(fu, 10.seconds)).toOption)
      case rc: RowCollection => if (rc.nonEmpty) Some(rc.tabulate().mkString("\n")) else None
      case sc: Scope => resolve(sc.toRowCollection)
      case tr: TableRendering => resolve(tr.toTable)
      case in: Instruction => Some(in.toSQL)
      case pr: Product => resolve(pr.toRowCollection)
      case s: String if s.trim.isEmpty => None
      case s: String if s.isQuoted => Some(s)
      case xx => Some(xx.renderAsJson)
    }
  }

  private def toAnchor(name: String): String = {
    name.map {
      case c if c.isLetterOrDigit => c
      case _ => '_'
    }.replace("__", "_")
  }

  private val array_comprehensions =
    """|['A' to 'F'].reverse()
       |""".stripMargin

  private val array_literals =
    """|['A', 'B', 'C', 'D', 'E', 'F'].reverse()
       |""".stripMargin

  private val dataframe_literals =
    """|from (
       ||----------------------------------------------------------|
       || exchange  | symbol | lastSale | lastSaleTime             |
       ||----------------------------------------------------------|
       || OTCBB     | SLZO   |   0.7004 | 2023-10-18T18:01:33.706Z |
       || NASDAQ    | BKM    |  43.1125 | 2023-10-18T18:01:05.769Z |
       || OTCBB     | POQF   |   0.7018 | 2023-10-18T18:01:45.085Z |
       || OTHER_OTC | EJDE   |   0.2156 | 2023-10-18T18:01:47.917Z |
       || OTCBB     | TZON   |   0.4941 | 2023-10-18T18:01:42.107Z |
       || NYSE      | BCM    |  79.8245 | 2023-10-18T18:01:06.778Z |
       || OTHER_OTC | JOXT   |   0.8961 | 2023-10-18T18:01:39.511Z |
       || OTHER_OTC | KFMP   |   0.8475 | 2023-10-18T18:01:16.714Z |
       || OTHER_OTC | UAWEN  |   0.7074 | 2023-10-18T18:01:57.225Z |
       || OTCBB     | CIYBJ  |   0.6753 | 2023-10-18T18:01:43.539Z |
       ||----------------------------------------------------------|
       |) where lastSale < 0.7 order by lastSale
       |""".stripMargin

  private val define_implicit_conversions =
    """|implicit class `java.lang.String` {
       |    def reverseString(self: String) := {
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

  private val charts_and_graphs =
    """|chart = { shape: "scatter", title: "Scatter Demo" }
       |samples = {
       |  import "java.lang.Math"
       |  def series(x) := "Series {{ (x % 2) + 1 }}"
       |  select w, x, y from ([0 to 500]
       |    .map(x => select w: series(x), x, y: x * iff((x % 2) is 0, Math.cos(x), Math.sin(x)))
       |    .toTable())
       |}
       |graph chart from samples
       |""".stripMargin

  private val dictionary_literals =
    """|response = { 'message1' : 'Hello World' }
       |response.message2 = 'Hallo Monde'
       |response
       |""".stripMargin

  private val fluent_collections =
    """|abc = [n => 2 * n, n => 3 * n, n => n * n]
       |[0 until abc.length()].map(x => abc(x)(4))
       |""".stripMargin

  private val function_literals =
    """|import "java.lang.Math"
       |pythagoros = (a, b) => Math.sqrt((a * a) + (b * b))
       |pythagoros(3, 4)
       |""".stripMargin

  private val import_implicit_conversions =
    """|import implicit "com.qwery.util.StringRenderHelper$StringRenderer"
       |DateTime().renderAsJson()
       |""".stripMargin

  private val json_literals =
    """|[{id: '7bd0b461-4eb9-400a-9b63-713af85a43d0', lastName: 'JONES', firstName: 'GARRY', airportCode: 'SNA'},
       | {id: '73a3fe49-df95-4a7a-9809-0bb4009f414b', lastName: 'JONES', firstName: 'DEBBIE', airportCode: 'SNA'},
       | {id: 'e015fc77-45bf-4a40-9721-f8f3248497a1', lastName: 'JONES', firstName: 'TAMERA', airportCode: 'SNA'},
       | {id: '33e31b53-b540-45e3-97d7-d2353a49f9c6', lastName: 'JONES', firstName: 'ERIC', airportCode: 'SNA'},
       | {id: 'e4dcba22-56d6-4e53-adbc-23fd84aece72', lastName: 'ADAMS', firstName: 'KAREN', airportCode: 'DTW'},
       | {id: '3879ba60-827e-4535-bf4e-246ca8807ba1', lastName: 'ADAMS', firstName: 'MIKE', airportCode: 'DTW'},
       | {id: '3d8dc7d8-cd86-48f4-b364-d2f40f1ae05b', lastName: 'JONES', firstName: 'SAMANTHA', airportCode: 'BUR'},
       | {id: '22d10aaa-32ac-4cd0-9bed-aa8e78a36d80', lastName: 'SHARMA', firstName: 'PANKAJ', airportCode: 'LAX'}
       |].toTable()
       |""".stripMargin

  private val matrices_and_vectors =
    """|vector = [2.0, 1.0, 3.0]
       |matrix = new Matrix([
       |  [1.0, 2.0, 3.0],
       |  [4.0, 5.0, 6.0],
       |  [7.0, 8.0, 9.0]
       |])
       |matrix * vector
       |""".stripMargin

  private val string_literals =
    """|item = { name : "Larry" }
       |'''|Hello {{ item.name }},
       |   |how are you?
       |   |Fine, I hope!
       |   |'''.stripMargin('|')
       |""".stripMargin

  private val instantiate_jvm_classes =
    """|// package com.github.ldaniels528.qwery
       |// case class StockQuote(symbol: String, exchange: String, lastSale: Double, lastSaleTime: Long)
       |
       |new `com.github.ldaniels528.qwery.StockQuote`(
       |    "ABC",
       |    "OTCBB",
       |    0.0231,
       |    DateTime().getTime()
       |)
       |""".stripMargin

  private val featuredExamples = List(
    "Array Literals" -> array_literals,
    "Array Comprehensions" -> array_comprehensions,
    "Fluent Arrays (supports map, filter, fold, etc.)" -> fluent_collections,
    "Charts and Graphs" -> charts_and_graphs,
    "Instantiate JVM classes" -> instantiate_jvm_classes,
    "Dataframe Literals" -> dataframe_literals,
    "Dictionary Literals" -> dictionary_literals,
    "Function Literals (Lambdas)" -> function_literals,
    "JSON Literals" -> json_literals,
    "Matrix and Vector Literals" -> matrices_and_vectors,
    "String Literals and Interpolation" -> string_literals,
    "Define (non-persistent) Implicit Classes" -> define_implicit_conversions,
    "Import (Scala-compiled) Implicit Classes" -> import_implicit_conversions
  )

}