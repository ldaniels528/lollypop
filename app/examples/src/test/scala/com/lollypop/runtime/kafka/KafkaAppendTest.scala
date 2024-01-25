package com.lollypop.runtime.kafka

import com.github.ldaniels528.lollypop.StockQuote
import com.lollypop.language.{LollypopUniverse, dieIllegalType}
import com.lollypop.runtime.{AutoClose, JSONProductConversion, LollypopVM, Scope, time}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringSerializer, UUIDSerializer}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import java.util.{Properties, UUID}
import scala.concurrent.duration.Duration.Inf
import scala.concurrent.{Await, Future}

/**
 * KafkaAppend Test Suite
 * @example {{{
 *   ./kafka/bin/kafka-consumer-group.sh --bootstrap-server 127.0.0.1:9090,127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095 \
 *   --topic stocks \
 *   --consumer.config ./kafka.properties \
 *    kafkaStocks456 **--reset-offsets** --to-offset 1 --all-topics --execute
 * }}}
 * @example {{{
 *  ./kafka/bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9090,127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095 \
 *    --topic myStocks --consumer.config ./kafka.properties
 * }}}
 * @example {{{
 *  ./kafka/bin/kafka-consumer-perf-test.sh --bootstrap-server 127.0.0.1:9090,127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095 \
 *    --topic stocks --consumer.config ./kafka.properties --messages 1000
 * }}}
 */
class KafkaAppendTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private val topic = "myStocks"
  private val groupId = "stocks567"
  private val partitionCount = 1
  private val universe = LollypopUniverse().withLanguageParsers(KafkaAppend)
  private val rootScope = Scope(universe)

  describe(classOf[KafkaAppend].getSimpleName) {

    it("should consumer Kafka messages and append them to a table") {
      scala.util.Properties.envOrNone("KAFKA") foreach { brokers =>
        logger.info(s"bootstrap.servers => $brokers")
        produceStocks(brokers, topic, count = 250)
        val (_, _, promise_?) = LollypopVM.executeSQL(rootScope,
          s"""|drop if exists KafkaStocks
              |create table KafkaStocks (
              |  symbol: String(8),
              |  exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
              |  lastSale: Double,
              |  lastSaleTime: Long
              |)
              |
              |val config = {
              |  "group.id": "$groupId",
              |  "bootstrap.servers": "$brokers",
              |  "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
              |  "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
              |  "enable.auto.commit": "true",
              |  "auto.commit.interval.ms": "2500"
              |}
              |kafkaAppend(ns('KafkaStocks'), "$topic", config, 250)
              |""".stripMargin)
        promise_? match {
          case null => fail("No future was returned")
          case promise: Future[Any] =>
            val result = Await.result(promise, Inf)
            assert(result == 250)
          case x => dieIllegalType(x)
        }
      }
    }

  }

  private def produceStocks(brokers: String, topic: String, count: Int = 100): Unit = {
    logger.info(s"Producing $count stock quotes...")
    new KafkaProducer[UUID, String](producerProperties(brokers)) use { producer =>
      for (n <- 0 until count) {
        val (record, msec) = time {
          val q = StockQuote.randomQuote
          val json = q.toJsValue.toString()
          logger.info(f"[$n%04d] $json")
          val record = new ProducerRecord(topic, n % partitionCount, System.currentTimeMillis(), UUID.randomUUID(), json)
          producer.send(record)
          record
        }
        logger.info(s"${record.key()} - [$msec msec]")
      }
      info(s"Produced $count records")
    }
  }

  private def producerProperties(brokers: String): Properties = {
    val props = new Properties()
    import ProducerConfig._
    props.put(BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(KEY_SERIALIZER_CLASS_CONFIG, classOf[UUIDSerializer].getName)
    props.put(VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(BATCH_SIZE_CONFIG, "100")
    props.put(LINGER_MS_CONFIG, "0")
    props.put(ACKS_CONFIG, "0")
    props
  }

}
