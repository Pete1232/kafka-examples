import java.util.Properties

import org.apache.kafka.clients.consumer.{Consumer, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class KafkaClient(topicName: String)
                 (implicit ec: ExecutionContext) {

  import KafkaClient._

  private lazy val producer: Producer[String, String] = new KafkaProducer[String, String](properties)

  private lazy val consumer: Consumer[String, String] = new KafkaConsumer[String, String](properties)
  consumer.subscribe(List(topicName).asJava)

  def create(k: String, v: String): Future[RecordMetadata] = Future {
    val meta = producer.send(new ProducerRecord[String, String](topicName, k, v)).get()
    producer.flush()
    meta
  }

  def read(): Future[Iterable[PrettyConsumerRecord]] = Future {
    val found = consumer.poll(3000)
    println(s"Found ${found.count()} records")
    val result = for (result <- found.asScala) yield {
      PrettyConsumerRecord(result.key(), result.value())
    }
    result
  }

  def close(): Unit = producer.close()
}


object KafkaClient {

  case class PrettyConsumerRecord(key: String, value: String)

  val properties: Properties = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")

    // write settings
    props.setProperty("key.serializer", classOf[StringSerializer].getName)
    props.setProperty("value.serializer", classOf[StringSerializer].getName)
    props.setProperty("acks", "1")
    props.setProperty("retries", "3")
    props.setProperty("linger.ms", "1") // record isn't sent until the client tells it to - either flush or this setting

    // read settings
    props.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer", classOf[StringDeserializer].getName)
    props.setProperty("group.id", "first_group")
    props.setProperty("enable.auto.commit", "true") //  defaults to true anyway
    props.setProperty("auto.commit.interval", "1")
    props.setProperty("auto.offset.reset", "earliest") // automatic reset when there is no initial offset

    props
  }
}
