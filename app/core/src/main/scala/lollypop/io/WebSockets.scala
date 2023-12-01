package lollypop.io

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage, WebSocketRequest}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{Materializer, OverflowStrategy}
import akka.util.ByteString
import akka.{Done, NotUsed}
import com.lollypop.util.StringRenderHelper.StringRenderer
import spray.json.JsValue

import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{Await, Future}

/**
 * WebSockets
 */
trait WebSockets {

  def connect(port: Int, callback: Any => Unit)(implicit system: ActorSystem, materializer: Materializer): WebSocket

  def connect(host: String, port: Int, callback: Any => Unit)(implicit system: ActorSystem, materializer: Materializer): WebSocket

}

/**
 * Represents a websocket client
 */
case class WebSocket(ws: ActorRef, connected: Future[_]) extends AutoCloseable {

  def awaitConnection(): Unit = Await.ready(connected, 10.seconds)

  def awaitConnection(timeout: Duration): Unit = Await.ready(connected, timeout)

  def send(data: Any): Unit = {
    val message = data match {
      case b: Array[Byte] => BinaryMessage.Strict(ByteString(b))
      case b: ByteString => BinaryMessage.Strict(b)
      case j: JsValue => TextMessage.Strict(j.compactPrint)
      case x => TextMessage.Strict(x.renderAsJson)
    }
    ws ! message
  }

  override def close(): Unit = ws ! PoisonPill

}

/**
 * WebSockets
 */
object WebSockets extends WebSockets {

  def apply(host: String = "127.0.0.1", port: Int, callback: Any => Unit): WebSocket = {
    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: Materializer = Materializer.matFromSystem
    connect(host, port, callback)
  }

  override def connect(port: Int, callback: Any => Unit)(implicit system: ActorSystem, materializer: Materializer): WebSocket = {
    connect(host = "127.0.0.1", port = port, callback)
  }

  override def connect(host: String, port: Int, callback: Any => Unit)(implicit system: ActorSystem, materializer: Materializer): WebSocket = {
    import system.dispatcher
    val request = WebSocketRequest(uri = s"ws://$host:$port/ws")
    val webSocketFlow = Http().webSocketClientFlow(request)
    val messageSource: Source[Message, ActorRef] = Source.actorRef[TextMessage.Strict](bufferSize = 500, OverflowStrategy.dropHead)
    val messageSink: Sink[Message, NotUsed] = Flow[Message].map {
      case message if message.isText && message.isStrict => callback(message.asTextMessage.getStrictText)
      case message => callback(message)
    }.to(Sink.ignore)

    val ((ws, upgradeResponse), _) = messageSource
      .viaMat(webSocketFlow)(Keep.both)
      .toMat(messageSink)(Keep.both)
      .run()

    // return the websocket handle
    WebSocket(ws, connected = upgradeResponse.flatMap {
      case upgrade if upgrade.response.status == StatusCodes.SwitchingProtocols => Future.successful(Done)
      case upgrade => Future.failed(new RuntimeException(s"Connection failed: ${upgrade.response.status}"))
    })
  }

}