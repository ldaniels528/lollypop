package com.lollypop.util

import com.lollypop.AppConstants.{DEFAULT_DATABASE, DEFAULT_SCHEMA}
import com.lollypop.util.OptionHelper.OptionEnrichment

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.{Failure, Properties, Success, Try}

/**
 * Console Reader Helper
 */
object ConsoleReaderHelper {

  def createAutomatedConsoleReader(commands: String*): () => String = {
    val inputs = commands.iterator
    () =>
      if (inputs.hasNext) {
        val command = inputs.next()
        println(command)
        command
      } else "exit"
  }

  def createInteractiveConsoleReader: () => String = {
    import java.io.{BufferedReader, InputStreamReader}
    val reader = new BufferedReader(new InputStreamReader(System.in))
    val sb = new mutable.StringBuilder(65536)

    () =>
      sb.clear()
      do {
        if (sb.isEmpty || reader.ready()) {
          val line = reader.readLine()
          if (line.nonEmpty) sb.append(line).append("\n")
        }
      } while (reader.ready())
      sb.toString()
  }

  /**
   * Facilitates the client conversation
   * @param database    the database name
   * @param schema      the schema name
   * @param console     the console reader function
   * @param executeCode the code execution function
   */
  @tailrec
  final def interactWith(database: String = DEFAULT_DATABASE,
                         schema: String = DEFAULT_SCHEMA,
                         console: () => String = createInteractiveConsoleReader,
                         executeCode: (String, String, String) => Try[(Option[String], Option[String])]): Unit = {
    import scala.Console._
    Console.print(s"$RESET$CYAN${Properties.userName}:/$database/$schema$RESET> ")
    val input = console().trim
    if (input.nonEmpty) Console.println(s"$RESET${YELLOW}Processing request... (Sent ${input.length} bytes)$RESET")
    input match {
      // ignore blank lines
      case "" => interactWith(database, schema, console, executeCode)
      // quit the CLI
      case "exit" => ()
      // execute a complete statement?
      case sql =>
        val (newDatabase, newSchema) = executeCode(database, schema, sql) match {
          case Success((database_?, schema_?)) => (database_? || database, schema_? || schema)
          case Failure(e) =>
            Console.err.println(e.getMessage + "\n")
            (database, schema)
        }
        interactWith(newDatabase, newSchema, console, executeCode)
    }
  }

}
