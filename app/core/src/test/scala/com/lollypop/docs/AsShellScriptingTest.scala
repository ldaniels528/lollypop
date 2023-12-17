package com.lollypop.docs

import com.lollypop.repl.LollypopREPL
import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime._

import java.io.{FileWriter, PrintWriter}

/**
 * As a Shell-Scripting Language Alternative document generator (shell-scripting.md)
 */
class AsShellScriptingTest extends DocumentGeneratorFunSpec {
  private val mdFile = docsDirectory / "shell-scripting.md"

  describe(classOf[AsShellScriptingTest].getSimpleName) {

    it(s"should generate ${mdFile.getPath}") {
      //implicit val scope: Scope = ctx.createRootScope()
      new PrintWriter(new FileWriter(mdFile)) use { out =>
        val unixCommands = LollypopREPL.languageParsers.flatMap(_.help).groupBy(_.name).toSeq.sortBy(_._1)
        val unixCommandNames = unixCommands.collect { case (name, h) if name.forall(_.isLetterOrDigit) =>
          name -> h.headOption.map(_.description).orNull
        }
        out.println(
          s"""|Lollypop offers developers the opportunity to use a Scala/SQL-hybrid scripting language for writing
              |shell-scripts. And because your scripts will be running within the JVM you can leverage Maven Central
              |and the myriads of libraries available to it.
              |
              |#### A new approach to shell-scripting
              |
              |Lollypop provides native variants of the following UNIX-like commands:
              |${unixCommandNames.map(h => s"* <a href=\"#${toAnchor(h._1)}\">${h._1}</a> - ${h._2}").mkString("\n")}
              |""".stripMargin)
        letsTrySomethingSimple(out)
      }
    }

  }

  private def letsTrySomethingSimple(out: PrintWriter): Unit = {
    out.println(
      """|#### Let's try something simple
         |
         |* What if I ask you to write some Bash code to retrieve the top 5 largest files by size in descending order?
         |
         |Could you write it without consulting a search engine or manual pages? Off the cuff, here's what I came up with to do it. It's crude, and it only works on the Mac...
         |
         |```bash
         |ls -lS ./app/examples/ | grep -v ^total | head -n 5
         |```
         |produced:
         |```text
         |-rw-r--r--@ 1 ldaniels  staff  4990190 Nov 11 23:50 stocks-100k.csv
         |-rw-r--r--@ 1 ldaniels  staff   336324 Nov 11 23:50 stocks.csv
         |-rw-r--r--@ 1 ldaniels  staff   249566 Nov 11 23:50 stocks-5k.csv
         |drwxr-xr-x@ 6 ldaniels  staff      192 Jul  5 14:57 target
         |drwxr-xr-x@ 5 ldaniels  staff      160 Jul  5 14:52 src
         |```
         |
         |And here's the equivalent in Lollypop:
         |
         |```sql
         |ls app/examples where not isHidden order by length desc limit 5
         |```
         |produced:
         |```sql
         ||-------------------------------------------------------------------------------------------------------------------------------------------------------|
         || name            | canonicalPath                                                | lastModified             | length  | isDirectory | isFile | isHidden |
         ||-------------------------------------------------------------------------------------------------------------------------------------------------------|
         || stocks-100k.csv | /Users/ldaniels/GitHub/lollypop/app/examples/stocks-100k.csv | 2023-11-12T07:50:27.490Z | 4990190 | false       | true   | false    |
         || stocks-5k.csv   | /Users/ldaniels/GitHub/lollypop/app/examples/stocks-5k.csv   | 2023-11-12T07:50:27.491Z |  249566 | false       | true   | false    |
         || .DS_Store       | /Users/ldaniels/GitHub/lollypop/app/examples/.DS_Store       | 2023-11-12T05:53:49.722Z |    6148 | false       | true   | true     |
         || target          | /Users/ldaniels/GitHub/lollypop/app/examples/target          | 2023-07-05T21:57:46.435Z |     192 | true        | false  | false    |
         || companylist     | /Users/ldaniels/GitHub/lollypop/app/examples/companylist     | 2023-11-12T07:50:27.476Z |     128 | true        | false  | false    |
         ||-------------------------------------------------------------------------------------------------------------------------------------------------------|
         |```
         |
         |* What if I ask for the same as above except find files recursively?
         |
         |```sql
         |find './app/examples/' where not isHidden order by length desc limit 5
         |```
         |```sql
         ||----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
         || name            | canonicalPath                                                                                             | lastModified             | length  | isDirectory | isFile | isHidden |
         ||----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
         || stocks-100k.csv | /Users/ldaniels/GitHub/lollypop/app/examples/stocks-100k.csv                                              | 2023-11-12T07:50:27.490Z | 4990190 | false       | true   | false    |
         ||----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
         |```
         |""".stripMargin)
  }

}
