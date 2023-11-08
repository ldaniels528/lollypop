package com.lollypop.database
package server

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.HttpCookiePair
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.scaladsl.Flow
import com.lollypop.AppConstants.{__port__, lollypopSessionID}
import com.lollypop.database.QueryResponse.{QueryResultConversion, RESULT_ROWS}
import com.lollypop.implicits.MagicImplicits
import com.lollypop.language.LollypopUniverse
import com.lollypop.language.models.Expression.implicits.LifestyleExpressionsAny
import com.lollypop.language.models._
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.ModelsJsonProtocol._
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.devices.RowCollection.dieColumnIndexOutOfRange
import com.lollypop.runtime.devices._
import com.lollypop.runtime.instructions.conditions.{AND, EQ}
import com.lollypop.runtime.instructions.functions.AnonymousFunction
import com.lollypop.runtime.instructions.queryables.RuntimeQueryable.DatabaseObjectRefDetection
import com.lollypop.runtime.instructions.queryables.Select
import com.lollypop.util.CodecHelper.EnrichedByteString
import com.lollypop.util.JSONSupport.JsValueConversion
import com.lollypop.util.OptionHelper.OptionEnrichment
import com.lollypop.util.ResourceHelper.AutoClose
import com.lollypop.util.StringRenderHelper.StringRenderer
import com.lollypop.{AppConstants, die}
import lollypop.io.IOCost
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory
import spray.json._

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer.wrap
import java.util.Base64
import scala.Console._
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Success, Try}

/**
 * Lollypop Server
 * @param port the port to bind
 * @param ctx  the root [[LollypopUniverse compiler context]]
 */
class LollypopServer(port: Int, ctx: LollypopUniverse = LollypopUniverse())(implicit system: ActorSystem) extends AutoCloseable {
  private val logger = LoggerFactory.getLogger(getClass)
  private val scopes = TrieMap[String, Scope]()
  private val apiRoutes = TrieMap[String, Map[String, Any]]()
  private val wwwRoutes = TrieMap[String, Map[String, String]]()
  private val systemStartupTime = System.currentTimeMillis()

  // pre-load the commands
  implicit val compiler: LollypopCompiler = LollypopCompiler()
  ctx.isServerMode = true

  private val rootScope: Scope = {
    val scope0 = ctx.createRootScope()
    val refs = for {
      (name, _type, value) <- Seq((__port__, Int32Type, port), ("server_sql", BooleanType, false))
    } yield Variable(name, _type, initialValue = value)
    refs.foldLeft(scope0) { (scope, ref) => scope.withVariable(ref) }
  }

  import system.dispatcher

  // start the listener
  val server: Future[Http.ServerBinding] = Http().newServerAt(interface = "0.0.0.0", port).bindFlow(route)
  server onComplete {
    case Success(serverBinding) =>
      logger.info(s"listening to ${serverBinding.localAddress}")
    case Failure(e) =>
      logger.error(s"Error: ${e.getMessage}", e)
  }

  def close(): Unit = {
    server.foreach(_.unbind())
  }

  /**
   * Creates a new API endpoint
   * @param url     the URL string
   * @param methods the HTTP method and code mapping
   * @return true, if a new endpoint was created
   */
  def createAPIEndPoint(url: String, methods: Map[String, Any]): Boolean = {
    apiRoutes.put(url, methods).isEmpty
  }

  /**
   * Creates a new HTML/CSS/File endpoint
   * @param files the HTTP URL to file mapping
   * @return true, if a new endpoint was created
   */
  def createFileEndPoint(url: String, files: Map[String, String]): Boolean = {
    logger.info(s"Registered '$url'...")
    wwwRoutes.put(url, files).nonEmpty
  }

  /**
   * Define the API routes
   * @return the [[Route]]
   */
  private def route: Route = {
    // Database API routes
    // route: /d/<database>/<schema>/<table> (e.g. "/d/shock_trade/portfolio/stocks")
    path("d" / Segment / Segment / Segment)(routesByDatabaseTable) ~
      // route: /d/<database>/<schema>/<table>/<rowID> (e.g. "/d/shock_trade/portfolio/stocks/187")
      path("d" / Segment / Segment / Segment / LongNumber)(routesByDatabaseTableRowID) ~
      // route: /d/<database>/<schema>/<table>/<rowID>/<columnID> (e.g. "/d/shock_trade/portfolio/stocks/187/2")
      path("d" / Segment / Segment / Segment / LongNumber / IntNumber)(routesByDatabaseTableColumnID) ~
      //
      // Message API routes
      // route: /m/<database>/<schema>/<table> (e.g. "/m/shock_trade/portfolio/stocks" <~ """{ "exchange":"OTCBB", "symbol":"EVRU", "lastSale":2.09, "lastSaleTime":1596403991000 }""")
      path("m" / Segment / Segment / Segment)(routesMessaging) ~
      // route: /m/<database>/<schema>/<table>/<rowID> (e.g. "/m/shock_trade/portfolio/stocks/187")
      path("m" / Segment / Segment / Segment / LongNumber)(routesByDatabaseTableRowID) ~
      //
      // Remote Table API routes
      // route: /col/<database>/<schema>/<table> (e.g. "/col/shock_trade/portfolio/stocks")
      path("col" / Segment / Segment / Segment)(routesByRemoteTableColumnServices) ~
      // route: /dnl/<database>/<schema>/<table> (e.g. "/dnl/shock_trade/portfolio/stocks")
      path("dnl" / Segment / Segment / Segment)(routesByRemoteTableDownloadServices) ~
      // route: /len/<database>/<schema>/<table> (e.g. "/len/shock_trade/portfolio/stocks")
      path("len" / Segment / Segment / Segment)(routesByRemoteTableLengthServices) ~
      // route: /fmd/<database>/<schema>/<table>/<rowID>/<columnID> (e.g. "/fmd/shock_trade/portfolio/stocks/1029/4")
      path("fmd" / Segment / Segment / Segment / LongNumber / IntNumber)(routesByRemoteTableFieldMetadataServices) ~
      // route: /rmd/<database>/<schema>/<table>/<rowID> (e.g. "/rmd/shock_trade/portfolio/stocks/1029")
      path("rmd" / Segment / Segment / Segment / LongNumber)(routesByRemoteTableRowServices) ~
      //
      // SQL/Query API routes
      // route: /q/<database> (e.g. "/q/shock_trade/portfolios")
      path("q" / Segment / Segment)(routesByDatabaseQuery) ~
      // route: /q (e.g. "/q")
      path("q")(routesByQuery) ~
      //
      // JDBC API routes
      // route: /jdbc/<database> (e.g. "/jdbc/stocks/portfolios")
      path("jdbc" / Segment / Segment)(routesByJDBC) ~
      //
      // Webservice API routes
      // route: /api/... (e.g. "/api/shock_trade/services/getStockQuote")
      pathPrefix("api" / Remaining)(routesByAPI) ~
      //
      // WebSockets API routes
      // route: /ws (e.g. "/ws")
      //pathPrefix("ws" / Remaining)(handleWebSocketMessages(routesByWS)) ~
      path("ws")(get(handleWebSocketMessages(webSocketHandler))) ~
      //
      // HTML/CSS/File routes
      // route: /www/... (e.g. "/www/notebook")
      pathPrefix("bower_components" / Remaining)(routesByWWW) ~
      pathPrefix("www" / Remaining)(routesByWWW) ~
      //
      // Authentication / Authorization API routes
      cookie(lollypopSessionID)(routesBySession)
  }

  private val webSocketHandler: Flow[Message, Message, NotUsed] = {
    var scope = Scope()
    Flow[Message].collect {
      case tm: TextMessage =>
        val response = apiRoutes.collectFirst { case (url, mappings) if mappings.exists { case (method, _) => method == "ws" } =>
          mappings("ws") match {
            case af: AnonymousFunction =>
              val (scopeA, _, resultA) = af.call(List(tm.getStrictText.v)).execute(scope)
              scope = scopeA
              resultA
            case code: Instruction =>
              val (scopeA, _, resultA) = code.execute(scope)
              scope = scopeA
              resultA
            case other =>
              other
          }
        }
        TextMessage(response.map(_.renderAsJson).getOrElse("null"))
      case bm: BinaryMessage =>
        logger.info(s"BinaryMessage: ${bm.getStrictData.toVector}")
        bm
    }
  }

  /**
   * Remote Table Column API routes (e.g. "/dnl/shock_trade/portfolio/stocks")
   * @param databaseName the name of the database
   * @param schemaName   the name of the schema
   * @param tableName    the name of the table
   * @return the [[Route]]
   */
  private def routesByRemoteTableColumnServices(databaseName: String, schemaName: String, tableName: String): Route = {
    withSession(databaseName, schemaName) { (_, scope0) =>
      get {
        // read the entire table (e.g. "GET /col/shock_trade/portfolio/stocks")
        val device = scope0.getRowCollection(DatabaseObjectNS(databaseName, schemaName, tableName))
        complete(device.columns.toJson)
      }
    }
  }

  /**
   * Remote Table Field-Metadata specific API routes (e.g. "/fmd/shock_trade/portfolio/stocks/1029/4")
   * @param databaseName the name of the database
   * @param schemaName   the name of the schema
   * @param tableName    the name of the table
   * @param rowID        the desired row by ID
   * @param columnID     the desired column by ID
   * @return the [[Route]]
   */
  private def routesByRemoteTableFieldMetadataServices(databaseName: String, schemaName: String, tableName: String, rowID: ROWID, columnID: Int): Route = {
    withSession(databaseName, schemaName) { (_, scope0) =>
      get {
        // read the field's metadata (e.g. "GET /fmd/shock_trade/portfolio/stocks/1029/4")
        val device = scope0.getRowCollection(DatabaseObjectNS(databaseName, schemaName, tableName))
        complete(device.readFieldMetadata(rowID, columnID).toJson)
      } ~ put {
        extract(_.request.uri.query()) { params =>
          // update the field's metadata (e.g. "PUT /fmd/shock_trade/portfolio/stocks/1029/4?fmd=32")
          val code = params.get("fmd").map(_.toByte) || die("Missing query parameter 'fmd'")
          val fmd = FieldMetadata.decode(code)
          val device = scope0.getRowCollection(DatabaseObjectNS(databaseName, schemaName, tableName))
          complete(device.updateFieldMetadata(rowID, columnID, fmd).toJson)
        }
      }
    }
  }

  /**
   * Remote Table Download API routes (e.g. "/dnl/shock_trade/portfolio/stocks")
   * @param databaseName the name of the database
   * @param schemaName   the name of the schema
   * @param tableName    the name of the table
   * @return the [[Route]]
   */
  private def routesByRemoteTableDownloadServices(databaseName: String, schemaName: String, tableName: String): Route = {
    withSession(databaseName, schemaName) { (_, scope0) =>
      get {
        // read the entire table (e.g. "GET /dnl/shock_trade/portfolio/stocks")
        val device = scope0.getRowCollection(DatabaseObjectNS(databaseName, schemaName, tableName))
        complete(device.encode)
      }
    }
  }

  /**
   * Remote Table-Row specific API routes (e.g. "/rmd/shock_trade/portfolio/stocks/1029")
   * @param databaseName the name of the database
   * @param schemaName   the name of the schema
   * @param tableName    the name of the table
   * @return the [[Route]]
   */
  private def routesByRemoteTableRowServices(databaseName: String, schemaName: String, tableName: String, rowID: ROWID): Route = {
    withSession(databaseName, schemaName) { (_, scope0) =>
      get {
        // read the row's metadata (e.g. "GET /rmd/shock_trade/portfolio/stocks/1029")
        val device = scope0.getRowCollection(DatabaseObjectNS(databaseName, schemaName, tableName))
        complete(device.readRowMetadata(rowID).toJson)
      } ~ put {
        extract(_.request.uri.query()) { params =>
          // update the row's metadata (e.g. "PUT /rmd/shock_trade/portfolio/stocks/1029?rmd=1")
          val code = params.get("rmd").map(_.toByte) || die("Missing query parameter 'rmd'")
          val rmd = RowMetadata.decode(code)
          val device = scope0.getRowCollection(DatabaseObjectNS(databaseName, schemaName, tableName))
          complete(device.updateRowMetadata(rowID, rmd).toJson)
        }
      }
    }
  }

  /**
   * Remote Table specific API routes (e.g. "/len/shock_trade/portfolio/stocks")
   * @param databaseName the name of the database
   * @param schemaName   the name of the schema
   * @param tableName    the name of the table
   * @return the [[Route]]
   */
  private def routesByRemoteTableLengthServices(databaseName: String, schemaName: String, tableName: String): Route = {
    withSession(databaseName, schemaName) { (_, scope0) =>
      get {
        // retrieve the database summary (e.g. "GET /len/shock_trade/public/StockQuotes")
        val device = scope0.getRowCollection(DatabaseObjectNS(databaseName, schemaName, tableName))
        complete(device.getLength.toJson)
      } ~ put {
        extract(_.request.uri.query()) { params =>
          // retrieve the database summary (e.g. "PUT /len/shock_trade/public/StockQuotes?size=98765")
          val newSize: Long = params.get("size").map(_.toLong) || die("Missing query parameter 'size'")
          val device = scope0.getRowCollection(DatabaseObjectNS(databaseName, schemaName, tableName))
          complete(device.setLength(newSize).toJson)
        }
      }
    }
  }

  /**
   * REST API routes (e.g. "/api/shock_trade/getStocks?symbol=XSD")
   * @param fullPath the API URL path (e.g. "/api/shock_trade/getStocks")
   * @return the [[Route]]
   */
  private def routesByAPI(fullPath: String): Route = {
    withSession() { scope =>
      extractMatchedPath { matchedPath =>
        val matchString = matchedPath.toString()

        def executeService(scopeA: Scope, method: String, input: Map[String, Any]): JsValue = {
          val methods = apiRoutes.getOrElse(matchString, die(s"Service binding for '$matchString' not found"))
          methods.get(method) flatMap {
            case instruction: Instruction =>
              val scopeB = scopeA.withVariable(name = "API_HTTP_METHOD", value = method.toUpperCase())
              val (scopeC, _, mixed) = instruction.execute(scopeB)
              Option(mixed) flatMap {
                case fx: AnonymousFunction =>
                  val converter: ParameterLike => Any => Expression = (p: ParameterLike) => {
                    val dataType = DataType.load(p.`type`)(scopeC)
                    (v: Any) => dataType.convert(v).v
                  }
                  val args = fx.params.toList.map(c => input.get(c.name).map(converter(c)).orNull)
                  Option(fx.call(args).execute(scopeC)._3)
                case op: Instruction => Option(op.execute(scopeC)._3)
                case x => instruction.dieIllegalType(x)
              }
            case option: Option[_] => option
            case other => Option(other)
          } match {
            case Some(jsValue: JsValue) => jsValue
            case v => v.toJson
          }
        }

        def executeParams(method: String, params: Uri.Query): JsValue =
          executeService(scope, method, input = params.toMap.map { case (name, value) => name -> value })

        def executeJS(method: String, jsObject: JsObject): JsValue =
          executeService(scope, method, input = jsObject.fields.map { case (name, value) => name -> value.unwrapJSON })

        def executeBoth(method: String, params: Uri.Query, jsObject: JsObject): JsValue = executeService(scope, method, input = {
          val m0 = params.toMap.map { case (name, value) => name -> value }
          val m1 = jsObject.fields.map { case (name, value) => name -> value.unwrapJSON }
          m0 ++ m1
        })

        delete(entity(as[JsObject]) { jsObject => complete(executeJS(method = "delete", jsObject)) }) ~
          get(extract(_.request.uri.query()) { params => complete(executeParams(method = "get", params)) }) ~
          head(extract(_.request.uri.query()) { params => complete(executeParams(method = "head", params)) }) ~
          options(extract(_.request.uri.query()) { params => complete(executeParams(method = "options", params)) }) ~
          patch(entity(as[JsObject]) { jsObject => complete(executeJS(method = "patch", jsObject)) }) ~
          post(entity(as[JsObject]) { jsObject =>
            extract(_.request.uri.query()) { params => complete(executeBoth(method = "post", params, jsObject)) }
          }) ~
          put(extract(_.request.uri.query()) { params => complete(executeParams(method = "put", params)) })
      }
    }
  }

  private def routesByWWW(filePath: String): Route = {
    extractMatchedPath { matchedPath =>
      val fullPath = matchedPath.toString()

      // attempt to find a matching route
      val route_? = wwwRoutes.collectFirst { case (app, mappings) if fullPath.startsWith(app) => app -> mappings }
      val (root, mappings) = route_? || die(s"No source root found for [$fullPath]")
      val uri = fullPath.drop(root.length)
      val path = mappings.get(uri) ?? mappings.get("*") || die(s"No mapping found for [$fullPath] in '$root'")

      // if the file is found locally, serve it
      val localFile = new File(path, uri)
      if (localFile.exists()) {
        logger.info(s"www: [$uri] => ${localFile.getPath} (${localFile.exists()})")
        processWWW(localFile)
      } else {
        // if the file is found in the classpath, cache it locally and serve it
        val resource = s"/$path/$uri".replaceAll("//", "/") ~> { s => if (s.endsWith("/")) s.dropRight(1) else s }
        val cachedFile = new File(new File("./temp"), resource)
        if (!cachedFile.exists() || cachedFile.length() == 0 || systemStartupTime > cachedFile.lastModified()) {
          val cachedDir = cachedFile.getParentFile
          if (!cachedDir.exists()) cachedDir.mkdirs()
          logger.info(s"www: '$resource' ~> '${cachedFile.getPath}' [${getClass.getResource(resource) != null}]")
          new FileOutputStream(cachedFile) use { out =>
            getClass.getResourceAsStream(resource).use(IOUtils.copy(_, out))
          }
        }
        processWWW(cachedFile)
      }
    }
  }

  private def processWWW(file: File): Route = {
    if (file.getName.toLowerCase().endsWith(".md")) {
      implicit val scope: Scope = Scope()
      val page = LollypopPage.fromFile(file)
      val fileContents = page.execute()._3
      complete(HttpResponse(entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, fileContents)))
    } else getFromFile(file)
  }

  /**
   * JDBC-specific API routes (e.g. "POST /jdbc/databaseName/schemaName" <~ { ... })
   * @param databaseName the name of the database
   * @param schemaName   the name of the schema
   * @return the [[Route]]
   */
  private def routesByJDBC(databaseName: String, schemaName: String): Route = {
    withSession(databaseName, schemaName) { (cookiePair, scope0) =>
      post {
        // setup the JDBC query with parameters (e.g. "select * from stocks where symbol is _0")
        entity(as[QueryRequest]) { case QueryRequest(sql, keyValues, limit) =>
          implicit val scope1: Scope = keyValues.foldLeft(scope0) {
            case (agg, (name, value)) => agg.withVariable(name, new String(value.fromBase64))
          }

          // execute the query
          val (scope3, route) = try runQuery(scope1, sql, limit) catch {
            case e: Exception =>
              e.printStackTrace()
              scope1.reset().withThrowable(e)
              scope1 -> complete(StatusCodes.InternalServerError.copy()(reason = cause(e).getMessage, defaultMessage = cause(e).getMessage))
          }
          cookiePair.foreach(cp => updateSession(cp.value, scope3))
          route
        }
      }
    }
  }

  /**
   * Database Query-specific API routes (e.g. "/q/shock_trade/portfolios")
   * @param databaseName the name of the database
   * @param schemaName   the name of the schema
   * @return the [[Route]]
   */
  private def routesByDatabaseQuery(databaseName: String, schemaName: String): Route = {
    withSession(databaseName, schemaName) { (cookiePair, scope0) =>
      post {
        extract(_.request.uri.query()) { params =>
          // execute the SQL query (e.g. "POST /q/databaseName/schemaName" <~ "truncate table staging")
          val limit = params.get("__limit").flatMap(v => Try(v.toInt).toOption)
          entity(as[String]) { sql =>
            val (scope1, outcome) = runQuery(scope0, sql, limit)
            cookiePair.foreach(cp => updateSession(cp.value, scope1))
            outcome
          }
        }
      }
    }
  }

  /**
   * Database Query-specific API routes (e.g. "/q")
   * @return the [[Route]]
   */
  private def routesByQuery: Route = {
    optionalCookie(lollypopSessionID) { cookiePair =>
      val scope = cookiePair.map(cp => getSession(cp.value)) || newSession

      post {
        extract(_.request.uri.query()) { params =>
          // execute the SQL query (e.g. "POST /q" <~ "truncate table staging")
          val limit = params.get("__limit").flatMap(v => Try(v.toInt).toOption)
          entity(as[String]) { sql =>
            val (scope1, outcome) = runQuery(scope, sql, limit)
            cookiePair.foreach(cp => updateSession(cp.value, scope1))
            outcome
          }
        }
      }
    }
  }

  private def runQuery(scope1: Scope, sql: String, limit: Option[Int]): (Scope, Route) = {
    val compiledCode = compiler.compile(sql)
    val ns_? = compiledCode.detectRef.collect { case ref: DatabaseObjectRef => ref.toNS(scope1) }
    val (scope2, cost1, result1) = compiledCode.execute(scope1)
    result1 match {
      case jsValue: JsValue => (scope2, complete(jsValue))
      case _ =>
        val outcome = Option(result1).map(_.toQueryResponse(ns_?, limit)(scope2))
        outcome match {
          case Some(results) => (scope2, complete(results))
          case None =>
            (scope2, complete(QueryResponse(
              resultType = RESULT_ROWS,
              ns = QueryResponse.getSource(ns_?)(scope2),
              cost = if (cost1 == IOCost.empty) None else Some(cost1),
              stdOut = scope2.getUniverse.system.stdOut.asString(),
              stdErr = scope2.getUniverse.system.stdErr.asString(),
            )))
        }
    }
  }

  @tailrec
  private def cause(t: Throwable): Throwable = if (t.getCause != null) cause(t.getCause) else t

  /**
   * Database Table-specific API routes (e.g. "/d/shock_trade/shock_trade/portfolios/stocks")
   * @param databaseName the name of the database
   * @param schemaName   the name of the schema
   * @param tableName    the name of the table
   * @return the [[Route]]
   */
  private def routesByDatabaseTable(databaseName: String, schemaName: String, tableName: String): Route = {
    withSession(databaseName, schemaName) { (_, scope0) =>
      implicit val scope1: Scope = scope0

      val ns = DatabaseObjectNS(databaseName, schemaName, tableName)
      implicit val device: RowCollection = scope0.getRowCollection(ns)
      delete {
        // drop the table by name (e.g. "delete /d/shock_trade/portfolio/stocks")
        complete(DatabaseManagementSystem.dropObject(ns, ifExists = true).toJson)
      } ~
        get {
          // retrieve the table metrics (e.g. "GET /d/shock_trade/portfolio/stocks")
          // or query via query parameters (e.g. "GET /d/shock_trade/portfolio/stocks?exchange=AMEX&__limit=5")
          extract(_.request.uri.query()) { params =>
            val (limit, condition) = (params.get("__limit").map(_.toInt) ?? Some(20), toCondition(params))
            if (params.isEmpty) {
              complete(
                DatabaseEntityMetrics(ns, columns = device.columns, physicalSize = Some(device.sizeInBytes),
                  recordSize = device.recordSize,
                  rows = device.getLength))
            }
            else complete {
              val (_, _, rows) = Select(fields = Seq(AllFields), from = Some(ns), where = condition, limit = limit.map(Literal.apply)).execute()
              rows.toMapGraph
            }
          }
        } ~
        post {
          // append the new record to the table
          // (e.g. "POST /d/shock_trade/portfolios/stocks" <~ { "exchange":"OTCBB", "symbol":"EVRU", "lastSale":2.09, "lastSaleTime":1596403991000 })
          entity(as[JsObject]) { jsObject =>
            complete(device.insert(toExpressions(jsObject)).toJson)
          }
        }
    }
  }

  /**
   * Database Table Field-specific API routes (e.g. "/d/shock_trade/shock_trade/portfolios/stocks/187/0")
   * @param databaseName the name of the database
   * @param schemaName   the name of the schema
   * @param tableName    the name of the table
   * @param rowID        the desired row by ID
   * @param columnID     the desired column by index
   * @return the [[Route]]
   */
  private def routesByDatabaseTableColumnID(databaseName: String, schemaName: String, tableName: String, rowID: ROWID, columnID: Int): Route = {
    withSession(databaseName, schemaName) { (_, scope0) =>
      val ns = DatabaseObjectNS(databaseName, schemaName, tableName)
      val device = scope0.getRowCollection(ns)
      val columns = device.columns
      assert(columns.indices isDefinedAt columnID, dieColumnIndexOutOfRange(columnID))
      val column = columns(columnID)

      delete {
        // delete a field (e.g. "delete /d/shock_trade/portfolios/stocks/287/0")
        complete(device.deleteField(rowID, columnID).toJson)
      } ~
        get {
          // retrieve a field (e.g. "GET /d/shock_trade/portfolios/stocks/287/0" ~> "CAKE")
          parameters(Symbol("__contentType").?) { contentType_? =>
            val field = device.readField(rowID, columnID)
            val fieldBytes = column.`type`.encode(field.value.orNull)
            complete({
              if (fieldBytes.isEmpty) HttpResponse(status = StatusCodes.NoContent)
              else sendFieldContents(column, fieldBytes, contentType_?)
            })
          }
        } ~
        put {
          // updates a field (e.g. "PUT /d/shock_trade/portfolios/stocks/287/3" <~ 124.56)
          entity(as[String]) { b64String =>
            val body = Base64.getDecoder.decode(b64String)
            val value = Option(column.`type`.decode(wrap(body)))
            complete(device.updateField(rowID, columnID, value).toJson)
          }
        }
    }
  }

  private def sendFieldContents(column: TableColumn, content: Array[Byte], contentType_? : Option[String]): HttpResponse = {
    val contentType = contentType_?.map(toContentType).getOrElse(toContentType(column.`type`))
    val response = HttpResponse(StatusCodes.OK, headers = Nil, entity = HttpEntity(contentType, content))
    response.entity
      .withContentType(contentType)
      .withSizeLimit(content.length)
    response
  }

  /**
   * Database Table Length-specific API routes (e.g. "/shock_trade/portfolio/stocks/187")
   * @param databaseName the name of the database
   * @param schemaName   the name of the schema
   * @param tableName    the name of the table
   * @param rowID        the referenced row ID
   * @return the [[Route]]
   */
  private def routesByDatabaseTableRowID(databaseName: String, schemaName: String, tableName: String, rowID: ROWID): Route = {
    withSession(databaseName, schemaName) { (_, scope0) =>
      implicit val scope1: Scope = scope0
      val ns = DatabaseObjectNS(databaseName, schemaName, tableName)
      implicit val device: RowCollection = scope0.getRowCollection(ns)
      delete {
        // delete the row by ID (e.g. "delete /d/shock_trade/portfolios/stocks/129")
        complete(device.delete(rowID).toJson)
      } ~
        get {
          // retrieve the row by ID (e.g. "GET /d/shock_trade/portfolios/stocks/287")
          try {
            complete(getRow(ns, rowID)(scope0) match {
              case Some(row) => row.toMap(recursive = true).toJson
              case None => JsObject()
            })
          } catch {
            case e: java.io.FileNotFoundException =>
              logger.warn(s"/d/$ns[$rowID] => ${e.getMessage}")
              complete(StatusCodes.NotFound)
            case e: Exception =>
              logger.warn(s"/d/$ns[$rowID] => ${e.getMessage}")
              complete(StatusCodes.InternalServerError)
          }
        } ~
        post {
          // partially update the row by ID
          // (e.g. "POST /d/stocks/287" <~ { "lastSale":2.23, "lastSaleTime":1596404391000 })
          entity(as[JsObject]) { jsObject =>
            complete(device.update(rowID, toExpressions(jsObject)).toJson)
          }
        } ~
        put {
          // replace the row by ID
          // (e.g. "PUT /d/stocks/287" <~ { "exchange":"OTCBB", "symbol":"EVRX", "lastSale":2.09, "lastSaleTime":1596403991000 })
          entity(as[JsObject]) { jsObject =>
            complete(device.update(rowID, toExpressions(jsObject)).toJson)
          }
        }
    }
  }

  /**
   * Messaging-specific API routes (e.g. "/m/shock_trade/stocks"
   * @param databaseName the name of the database
   * @param schemaName   the name of the schema
   * @param tableName    the name of the table
   * @return the [[Route]]
   */
  private def routesMessaging(databaseName: String, schemaName: String, tableName: String): Route = {
    withSession(databaseName, schemaName) { (_, scope0) =>
      val ns = DatabaseObjectNS(databaseName, schemaName, tableName)
      implicit val device: RowCollection = scope0.getRowCollection(ns)
      implicit val scope1: Scope = scope0
      post {
        // append the new message to the table
        // (e.g. "POST /m/stocks.amex" <~ { "exchange":"OTCBB", "symbol":"EVRU", "lastSale":2.09, "lastSaleTime":1596403991000 })
        entity(as[JsObject]) { jsObject =>
          complete(device.insert(toExpressions(jsObject)).toJson)
        }
      }
    }
  }

  private def routesBySession(cookiePair: HttpCookiePair): StandardRoute = {
    if (!scopes.contains(cookiePair.value)) {
      implicit val scope: Scope = newSession
      scopes += cookiePair.value -> scope
    }
    complete(JsObject())
  }

  def shutdown(hardDeadline: FiniteDuration = 5.seconds): Unit = {
    server.foreach(_.terminate(hardDeadline))
  }

  /**
   * Retrieves a row by ID
   * @param ns    the [[DatabaseObjectNS table reference]]
   * @param rowID the row ID
   * @return the option of a [[Row row]]
   */
  private def getRow(ns: DatabaseObjectNS, rowID: ROWID)(implicit scope: Scope): Option[Row] = {
    val row = scope.getRowCollection(ns).apply(rowID)
    if (row.metadata.isActive) Some(row) else None
  }

  private def getSession(sessionID: String): Scope = {
    implicit val scope: Scope = scopes.getOrElseUpdate(sessionID, Scope(rootScope))
    if (!scope.getValueReferences.contains("sessionID"))
      scope.withVariable(name = "sessionID", `type` = UUIDType, value = Some(sessionID), isReadOnly = false)
    scope
  }

  private def newSession: Scope = {
    val scope = Scope(rootScope)
    scope
  }

  private def updateSession(sessionID: String, scope: Scope): Scope = {
    scopes.put(sessionID, scope)
    scope
  }

  private def withSession()(block: Scope => Route): Route = {
    optionalCookie(lollypopSessionID) { cookiePair =>
      implicit val scope0: Scope = cookiePair.map(cp => getSession(cp.value)) || newSession
      block(scope0)
    }
  }

  private def withSession(databaseName: String, schemaName: String)(block: (Option[HttpCookiePair], Scope) => Route): Route = {
    optionalCookie(lollypopSessionID) { cookiePair =>
      val scope0: Scope = (cookiePair.map(cp => getSession(cp.value)) || newSession)
        .withDatabase(databaseName)
        .withSchema(schemaName)
      block(cookiePair, scope0)
    }
  }

  private def toCondition(params: Map[String, Any]): Option[Condition] = {
    if (params.isEmpty) None
    else {
      params.foldLeft[Option[Condition]](None) { case (condition, (name, value)) =>
        val newCondition = EQ(FieldRef(name), Literal(value))
        condition match {
          case None => Some(newCondition)
          case Some(oldCondition) => Some(AND(oldCondition, newCondition))
        }
      }
    }
  }

  private def toCondition(params: Uri.Query): Option[Condition] = {
    val mapping = params.filterNot(_._1.name.startsWith("__")).toMap
    toCondition(mapping)
  }

  private def toContentType(`type`: String): ContentType = {
    `type`.toLowerCase() match {
      case "binary" => ContentTypes.`application/octet-stream`
      case "csv" => ContentTypes.`text/csv(UTF-8)`
      case "json" => ContentTypes.`application/json`
      case "html" => ContentTypes.`text/html(UTF-8)`
      case "text" => ContentTypes.`text/plain(UTF-8)`
      case "xml" => ContentTypes.`text/xml(UTF-8)`
      case _ => toContentType(`type` = StringType)
    }
  }

  private def toContentType(`type`: DataType): ContentType = {
    `type`.name match {
      case s if s == ArrayType.name => ContentTypes.`application/octet-stream`
      case s if s == BlobType.name => ContentTypes.`application/octet-stream`
      case s if s == ClobType.name => ContentTypes.`text/html(UTF-8)`
      case s if s == StringType.name => ContentTypes.`text/html(UTF-8)`
      case _ => //ContentTypes.`text/plain(UTF-8)`
        ContentTypes.`application/octet-stream`
    }
  }

  private def toExpressions(jsObject: JsObject)(implicit scope: Scope, rc: RowCollection): Row = {
    Map(jsObject.fields.toSeq.map { case (k, js) =>
      k -> Option(js.toExpression.execute(scope)._3)
    }: _*).toRow
  }

}

/**
 * Lollypop Server Companion
 */
object LollypopServer {
  private val defaultActorPoolName = "lollypop-server"

  def apply(port: Int): LollypopServer = {
    implicit val system: ActorSystem = ActorSystem(name = defaultActorPoolName)
    new LollypopServer(port)
  }

  def apply(port: Int, ctx: LollypopUniverse): LollypopServer = {
    implicit val system: ActorSystem = ActorSystem(name = defaultActorPoolName)
    new LollypopServer(port, ctx)
  }

  def apply(port: Int, ctx: LollypopUniverse, system: ActorSystem): LollypopServer = {
    new LollypopServer(port, ctx)(system)
  }

  /**
   * Main program
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    val defaultPort = 8233

    // display the application version
    val version = AppConstants.version
    Console.println(s"$RESET${GREEN}QW${MAGENTA}E${RED}R${BLUE}Y Server ${CYAN}v$version$RESET")
    Console.println()

    // get the configuration
    val port: Int = args.toList match {
      case port :: _ => port.toInt
      case _ => defaultPort
    }

    // start the server
    LollypopServer(port)
  }

}
