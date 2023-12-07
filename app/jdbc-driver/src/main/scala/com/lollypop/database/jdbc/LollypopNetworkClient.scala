package com.lollypop.database.jdbc

import com.lollypop.database.server.LollypopServer
import com.lollypop.die
import com.lollypop.language._
import com.lollypop.repl.LollypopREPL.getResourceFile
import com.lollypop.runtime._
import com.lollypop.util.ConsoleReaderHelper.{createInteractiveConsoleReader, interactWith}

import java.io.File
import java.sql.{Connection, Driver, DriverManager}
import scala.annotation.tailrec
import scala.io.{Source, StdIn}
import scala.language.implicitConversions
import scala.util.{Properties, Try}

/**
 * Lollypop Network Client
 */
trait LollypopNetworkClient {

  /**
   * Facilitates the client conversation
   * @param conn    the [[Connection connection]]
   * @param console the console reader
   */
  def interact(implicit conn: Connection, console: () => String = createInteractiveConsoleReader): Unit = {
    // include the ~/.lollypoprc file (if it exists)
    val rcFile = new File(Properties.userHome, ".lollypoprc")
    getResourceFile(rcFile).map(conn.createStatement().executeQuery)

    // process client requests
    try interactWith(
      database = Option(conn.getCatalog) || DEFAULT_DATABASE,
      schema = Option(conn.getSchema) || DEFAULT_SCHEMA,
      console = console,
      executeCode = { (_, _, sql) =>
        Try(conn.createStatement().executeQuery(sql)) map { rs =>
          rs.tabulate() foreach Console.println
          val metaData = rs.getMetaData
          (Option(metaData.getCatalogName(1)), Option(metaData.getSchemaName(1)))
        }
      })
    finally conn.close()
  }

}

/**
 * Lollypop Network Client
 */
object LollypopNetworkClient extends LollypopNetworkClient {
  import scala.Console._

  private val choices = Seq(
    s"Start ${GREEN}dedicated Lollypop Server$RESET",
    s"Start CLI with ${GREEN}embedded Lollypop Server$RESET",
    s"Start CLI and connect to ${BLUE}remote Lollypop Server$RESET",
    s"Start CLI and connect to ${YELLOW}generic JDBC Database$RESET",
    s"Start SQL script in ${MAGENTA}standalone mode$RESET")

  /**
   * For commandline execution
   * @param args the commandline arguments
   */
  def main(args: Array[String]): Unit = {
    import scala.Console._
    Console.println(s"$RESET${GREEN}Q${CYAN}W${MAGENTA}E${RED}R${BLUE}Y$YELLOW CLI v$version$RESET")
    Console.println()
    args match {
      case Array("--jdbc", jdbcDriver, jdbcURL) => startCliWithGenericJDBC(jdbcDriver, jdbcURL)
      case Array(host, port) if port.matches("\\d+") => startCliWithRemoteServer(host, port.toInt)
      case Array(port) if port.matches("\\d+") => startCliWithEmbeddedServer(host = "0.0.0.0", port.toInt)
      case _ => menu()
    }
  }

  @tailrec
  private def menu(selection: Int = 0): Unit = {
    selection match {
      case 0 =>
        Console.println("Entering interactive mode.")
        Console.println("Choose one of the following startup options:")
        choices.zipWithIndex foreach { case (text, n) => Console.println(f"${n + 1}. $text") }
        Console.print(s"Choice (${GREEN}1$RESET..$GREEN${choices.length}$RESET)> ")
        menu(selection = StdIn.readInt())
      case 1 => startDedicatedServer()
      case 2 => startCliWithEmbeddedServer()
      case 3 => startCliWithRemoteServer()
      case 4 => startCliWithGenericJDBC()
      case 5 => startScript()
      case _ => die(s"Expected input between '1' and '${choices.length}'")
    }
  }

  private def startDedicatedServer(port: Int = readInt("Server port: ")): LollypopServer = {
    Console.println(s"Starting Local Database Server on port $port...")
    LollypopServer(port)
  }

  private def startCliWithEmbeddedServer(host: String = "0.0.0.0", port: Int = readInt("Server port: ")): Unit = {
    Console.println(s"Starting Local Database Server on port $port...")
    LollypopServer(port) use { _ =>
      getConnection(jdbcDriver = classOf[LollypopDriver].getName, jdbcURL = createURL(host, port)) use { conn =>
        interact(conn)
      }
    }
  }

  private def startCliWithGenericJDBC(jdbcDriver: String = readString("JDBC Driver: "),
                                      jdbcURL: String = readString("JDBC URL: ")): Unit = {
    getConnection(jdbcDriver, jdbcURL) use { conn => interact(conn) }
  }

  private def startCliWithRemoteServer(host: String = readString("Host: "),
                                       port: Int = readInt("Port: ")): Unit = {
    getConnection(jdbcDriver = classOf[LollypopDriver].getName, jdbcURL = createURL(host, port)) use { conn => interact(conn) }
  }

  def startScript(): Unit = {
    val host = readString("Host: ")
    val port = readInt("Port: ")
    val file = readString("SQL file: ")
    val scriptFile = new File(file)
    Console.println(s"Executing SQL file '${scriptFile.getAbsolutePath}'...")
    getConnection(classOf[LollypopDriver].getName, createURL(host, port)) use { implicit conn =>
      startScript(scriptFile)
    }
  }

  def startScript(scriptFile: File)(implicit conn: Connection): Unit = {
    val code = Source.fromFile(scriptFile).use(_.mkString)
    val rs = conn.createStatement().executeQuery(code)
    rs.tabulate() foreach Console.println
  }

  def createURL(host: String, port: Int): String = {
    s"jdbc:lollypop://$host:$port/$DEFAULT_DATABASE.$DEFAULT_SCHEMA"
  }

  def getConnection(jdbcDriver: String, jdbcURL: String): Connection = {
    Console.println(s"Connecting to '$jdbcURL' [$jdbcDriver]...")
    Class.forName(jdbcDriver).getDeclaredConstructor().newInstance() match {
      case driver: Driver => DriverManager.registerDriver(driver)
      case _ =>
    }
    DriverManager.getConnection(jdbcURL)
  }

  private def readInt(label: String): Int = {
    Console.print(label)
    StdIn.readInt()
  }

  private def readString(label: String): String = {
    Console.print(label)
    StdIn.readLine().trim
  }

}
