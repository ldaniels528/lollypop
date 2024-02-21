package lollypop.io

import com.lollypop.database.QueryResponse
import com.lollypop.database.server.LollypopServer
import com.lollypop.die
import com.lollypop.language.HelpDoc.{CATEGORY_CONCURRENCY, PARADIGM_DECLARATIVE, PARADIGM_FUNCTIONAL}
import com.lollypop.language.{HelpDoc, HelpIntegration, LollypopUniverse}
import com.lollypop.repl.LollypopREPL

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.util._

/**
 * Represents a Lollypop server node manager
 * @example {{{
 *  // Lollypop version
 *  node = Nodes.start()
 *  node.awaitStartup(Duration('1 second'))
 *  node.exec(
 *  """|from (
 *     ||-------------------------------------------------------|
 *     || ticker | market | lastSale | lastSaleTime             |
 *     ||-------------------------------------------------------|
 *     || NKWI   | OTCBB  |  98.9501 | 2022-09-04T23:36:47.846Z |
 *     || AQKU   | NASDAQ |  68.2945 | 2022-09-04T23:36:47.860Z |
 *     || WRGB   | AMEX   |  46.8355 | 2022-09-04T23:36:47.862Z |
 *     || ESCN   | AMEX   |  42.5934 | 2022-09-04T23:36:47.865Z |
 *     || NFRK   | AMEX   |  28.2808 | 2022-09-04T23:36:47.864Z |
 *     ||-------------------------------------------------------|
 *     |) where lastSale < 30
 *     |""".stripMargin('|'))
 * }}}
 * @example {{{
 *  // Scala version
 *  import scala.concurrent.duration.DurationInt
 *  val node = Nodes().start()
 *  node.awaitStartup(1.second))
 *  node.exec(
 *  """|from (
 *     ||-------------------------------------------------------|
 *     || ticker | market | lastSale | lastSaleTime             |
 *     ||-------------------------------------------------------|
 *     || NKWI   | OTCBB  |  98.9501 | 2022-09-04T23:36:47.846Z |
 *     || AQKU   | NASDAQ |  68.2945 | 2022-09-04T23:36:47.860Z |
 *     || WRGB   | AMEX   |  46.8355 | 2022-09-04T23:36:47.862Z |
 *     || ESCN   | AMEX   |  42.5934 | 2022-09-04T23:36:47.865Z |
 *     || NFRK   | AMEX   |  28.2808 | 2022-09-04T23:36:47.864Z |
 *     ||-------------------------------------------------------|
 *     |) where lastSale < 30
 *     |""".stripMargin)
 * }}}
 */
class Nodes(val ctx: LollypopUniverse) {
  private val nodes = TrieMap[Int, Node]()

  def apply(port: Int): Node = getOrCreateNode(port)

  def exec(port: Int, statement: String): QueryResponse = {
    val node = nodes.getOrElse(port, die(s"No peer registered on port $port"))
    node.exec(statement)
  }

  def getNode(port: Int): Option[Node] = nodes.get(port)

  def getOrCreateNode(port: Int): Node = nodes.getOrElseUpdate(port, Node(server = LollypopServer(port, ctx)))

  def peers: Array[Int] = nodes.keys.toArray

  def peerNodes: Array[Node] = nodes.values.toArray

  /**
   * Starts a new node
   * @return a new [[Node node]]
   */
  def start(): Node = start(timeout = Duration.Inf)

  /**
   * Starts a new node
   * @param timeout the [[Duration timeout duration]]
   * @return a new [[Node node]]
   */
  def start(timeout: Duration): Node = {
    val random = new Random(System.currentTimeMillis())

    @tailrec
    def startUpOnRandomPort(): Node = {
      val port = random.nextInt(8000) + 8000
      if (nodes.contains(port)) startUpOnRandomPort() else startManually(port)(timeout)
    }

    startUpOnRandomPort()
  }

  private def startManually(port: Int)(implicit timeout: Duration): Node = {
    val node = getOrCreateNode(port)
    node.awaitStartup(timeout)
  }

}

object Nodes extends HelpIntegration {

  def apply(ctx: LollypopUniverse = LollypopUniverse().withLanguageParsers(LollypopREPL.languageParsers: _*)): Nodes = {
    new Nodes(ctx)
  }

  override def help: List[HelpDoc] = List(
    HelpDoc(
      name = "Nodes",
      category = CATEGORY_CONCURRENCY,
      paradigm = PARADIGM_DECLARATIVE,
      description = "Executes a statement on a running Lollypop peer node.",
      example =
        """|node = Nodes.start()
           |node.awaitStartup(Duration('1 second'))
           |results = node.exec('''
           |from (
           ||-------------------------------------------------------|
           || ticker | market | lastSale | lastSaleTime             |
           ||-------------------------------------------------------|
           || NKWI   | OTCBB  |  98.9501 | 2022-09-04T23:36:47.846Z |
           || AQKU   | NASDAQ |  68.2945 | 2022-09-04T23:36:47.860Z |
           || WRGB   | AMEX   |  46.8355 | 2022-09-04T23:36:47.862Z |
           || ESCN   | AMEX   |  42.5934 | 2022-09-04T23:36:47.865Z |
           || NFRK   | AMEX   |  28.2808 | 2022-09-04T23:36:47.864Z |
           ||-------------------------------------------------------|
           |) where lastSale < 30
           |''')
           |node.stop()
           |results
           |""".stripMargin
    ), HelpDoc(
      name = "Nodes",
      category = CATEGORY_CONCURRENCY,
      paradigm = PARADIGM_FUNCTIONAL,
      description = "Creates a new REST API endpoint.",
      example =
        """|node = Nodes.start()
           |node.awaitStartup(Duration('1 second'))
           |node.api('/api/comments/', {
           |  post: (message: String) => "post '{{message}}'"
           |  get: (id: UUID) => "get {{(id}}"
           |  put: (id: UUID, message: String) => "put '{{message}}' ~> {{(id}}"
           |  delete: (id: UUID) => "delete {{(id}}"
           |})
           |www post "http://0.0.0.0:{{node.server.port()}}/api/comments/" <~ { message: "Hello World" }
           |""".stripMargin
    ), HelpDoc(
      name = "Nodes",
      category = CATEGORY_CONCURRENCY,
      paradigm = PARADIGM_DECLARATIVE,
      description =
        """|Opens a commandline interface to a remote Lollypop peer node.
           |""".stripMargin,
      example =
        """|node = Nodes.start()
           |node.awaitStartup(Duration('1 second'))
           |try
           |  node.exec(["x = 1", "y = 2", "z = x + y", "z"])
           |catch e => stderr <=== e
           |finally
           |  node.stop()
           |""".stripMargin
    ), HelpDoc(
      name = "Nodes",
      category = CATEGORY_CONCURRENCY,
      paradigm = PARADIGM_DECLARATIVE,
      description = "Creates a new HTML/CSS/File endpoint",
      example =
        """|node = Nodes.start()
           |node.awaitStartup(Duration('1 second'))
           |node.files('/www/notebooks/', {
           |  "" : "public/index.html",
           |  "*" : "public"
           |})
           |""".stripMargin
    ))

}