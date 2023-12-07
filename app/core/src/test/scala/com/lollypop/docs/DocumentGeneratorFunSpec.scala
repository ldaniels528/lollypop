package com.lollypop.docs

import com.lollypop.database.QueryResponse
import com.lollypop.database.server.LollypopChartGenerator
import com.lollypop.language._
import com.lollypop.language.models.Instruction
import com.lollypop.repl.LollypopREPL
import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.instructions.expressions.GraphResult
import com.lollypop.runtime.instructions.queryables.TableRendering
import com.lollypop.runtime.{LollypopVM, Scope}
import com.lollypop.util.StringRenderHelper.{StringRenderer, toProductString}
import org.scalatest.funspec.AnyFunSpec

import java.io.{File, PrintWriter, StringWriter}
import scala.annotation.tailrec
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.language.reflectiveCalls
import scala.util.{Failure, Success, Try}

abstract class DocumentGeneratorFunSpec extends AnyFunSpec {
  protected implicit val ctx: LollypopUniverse = LollypopUniverse(isServerMode = true)
    .withLanguageParsers(LollypopREPL.languageParsers: _*)
  protected val baseDirectory = new File(".")
  protected val docsDirectory: File = baseDirectory / "docs"
  protected val imageDirectory: File = docsDirectory / "images"

  protected def invoke(out: PrintWriter, help: HelpDoc)(implicit scope: Scope): Unit = {
    val title = help.featureTitle.getOrElse("???")
    // header section
    out.println(
      s"""|<a name="${toAnchor(title)}"></a>
          |### $title
          |*Description*: ${help.description.trim}
          |""".stripMargin)
    out.println("```sql")
    out.println(help.example.trim)
    out.println("```")

    // detail section
    for {
      (scope1, _, result1) <- Try(LollypopVM.executeSQL(scope, help.example))
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

  protected def invoke(out: PrintWriter, help: HelpDoc, nth: String)(implicit scope: Scope): Unit = {
    // header section
    out.println(
      s"""|### ${help.name}$nth (${help.category} &#8212; ${help.paradigm}) ${handleExperimental(help)}
          |*Description*: ${help.description.trim}
          |""".stripMargin)
    out.println("```sql")
    out.println(help.example.trim)
    out.println("```")

    // detail section
    Try(LollypopVM.executeSQL(scope, help.example)) match {
      case Failure(e) =>
        out.println("```scala")
        out.println({
          val sw = new StringWriter()
          e.printStackTrace(new PrintWriter(sw))
          sw.getBuffer.toString
        })
        out.println("```")
        showConsoleOutputs(out, scope)

      case Success((scope1, _, result1)) =>
        out.println("##### Results")
        resolve(result1) match {
          case Some(results) =>
            if (results.contains("<img")) out.println(results)
            else {
              out.println("```sql")
              out.println(results)
              out.println("```")
            }
            showConsoleOutputs(out, scope1)
          case None =>
        }
    }
  }

  private def handleExperimental(help: HelpDoc): String = {
    if (help.isExperimental) {
      val iconFile = imageDirectory / "flask.svg"
      s"""<img src="${iconFile.getPath}" width="24" height="24">"""
    } else ""
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

  @tailrec
  private def resolve(outcome: Any)(implicit scope: Scope): Option[String] = {
    outcome match {
      case null | None => None
      case Some(rc: RowCollection) => resolve(rc)
      case dr: GraphResult =>
        val imageFile = LollypopChartGenerator.generateFile(imageDirectory, dr)
        Some(
          s"""|<div style="width: 100%">
              |<img src="${imageFile.getPath}">
              |</div>
              |""".stripMargin)
      case fu: Future[_] => resolve(Try(Await.result(fu, 10.seconds)).toOption)
      case rc: RowCollection => if (rc.nonEmpty) Some(rc.tabulate().mkString("\n")) else None
      case qr: QueryResponse => Some(qr.tabulate().mkString("\n"))
      case sc: Scope => resolve(sc.toRowCollection)
      case tr: TableRendering => resolve(tr.toTable)
      case in: Instruction => Some(in.toSQL)
      case pr: Product => Some(toProductString(pr))
      case s: String if s.trim.isEmpty => None
      case s: String if s.isQuoted => Some(s)
      case xx => Some(xx.render)
    }
  }

  protected def toAnchor(name: String): String = {
    name.map {
      case c if c.isLetterOrDigit => c
      case _ => '_'
    }.replace("__", "_")
  }

}
