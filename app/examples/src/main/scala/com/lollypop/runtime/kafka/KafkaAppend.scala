package com.lollypop.runtime.kafka

import com.lollypop.language.models.Expression
import com.lollypop.runtime.devices._
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.instructions.functions.{FunctionCallParserE3Or4, ScalarFunctionCall}
import com.lollypop.runtime.{ExpressiveTypeConversion, MapToRow, _}
import lollypop.io.IOCost
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.Properties
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

/**
 * Sets up a thread to continuously appends messages from a Kafka topic to a table.
 * @param tableExpr  the target table [[Expression expression]]
 * @param topicExpr  the Kafka topic [[Expression expression]]
 * @param configExpr the Kafka configuration [[Expression expression]]
 * @param limitExpr  an optional limit [[Expression expression]]
 */
case class KafkaAppend(tableExpr: Expression, topicExpr: Expression, configExpr: Expression, limitExpr: Option[Expression] = None)
  extends ScalarFunctionCall with RuntimeExpression {
  private val logger = LoggerFactory.getLogger(getClass)

  override def execute()(implicit scope: Scope): (Scope, IOCost, Future[Long]) = {
    val (sa, ca, rc) = tableExpr.search(scope)
    val (sb, cb, topic) = topicExpr.pullString(sa)
    val (sc, cc, config) = configExpr.pullDictionary(sb)
    val (sd, cd, limit_?) = limitExpr.map(_.pullLong(sc).map(x => Some(x))) getOrElse(sc, cc, None)
    (sd, ca ++ cb ++ cc ++ cd, consumeMessages(rc, topic = Seq(topic), config.toMap, limit = limit_?))
  }

  private def consumeMessages(rc: RowCollection, topic: Seq[String], config: QMap[_, _], limit: Option[Long]): Future[Long] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val topics = topic.asJava
    Future {
      new KafkaConsumer[String, String](consumerProperties(config)) use { consumer =>
        consumer.subscribe(topics)
        var processed: Long = 0
        while (limit.isEmpty || limit.exists(_ > processed)) {
          val records = consumer.poll(Duration.ofMillis(2500)).asScala
          for (rec <- records) {
            processed += 1
            rec.value() match {
              case jsonString: String =>
                logger.info(jsonString)
                jsonString.parseJSON.unwrapJSON match {
                  case m: QMap[_, _] =>
                    val row = (m.collect { case (k: String, v) => k -> v } ++ Map("__KAFKA_MSG_ID" -> rec.key())).toMap.toRow(rc)
                    rc.insert(row)
                  case x => dieIllegalType(x)
                }
              case x =>
                logger.error(s"Unhandled message: '$x'")
                dieIllegalType(x)
            }
          }
        }
        logger.info("Shutting down...")
        processed
      }
    }
  }

  private def consumerProperties(config: QMap[_, _]): Properties = {
    val props = new Properties()
    config foreach {
      case (key: String, values: Array[String]) => props.put(key, values.mkString(","))
      case (key: String, value: AnyRef) => props.put(key, value)
      case x => dieIllegalType(x)
    }
    props
  }

}


object KafkaAppend extends FunctionCallParserE3Or4(
  name = "kafkaAppend",
  description =
    """|Continuously appends messages from a Kafka topic into a table.
       |""".stripMargin,
  example =
    """|kafkaAppend(@@stocks, "com_acme_stocks", {
       | "group.id": "test",
       | "bootstrap.servers": "localhost:9092",
       | "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
       | "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
       | "enable.auto.commit": "true",
       | "auto.commit.interval.ms": "1000"
       |})
       |""".stripMargin)
