import java.time.temporal.ChronoUnit
import java.time.{OffsetDateTime, ZoneOffset}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.Try

object Main extends App {

  val runTimeInSeconds = 5

  val start = OffsetDateTime.now(ZoneOffset.UTC)

  val client = new KafkaClient("first_topic")

  import client._

  val message = Try(args(0)).getOrElse("Test Only")

  println("Written to topic " + Await.result(create("first_key", message), Duration.Inf).topic())

  while (ChronoUnit.SECONDS.between(start, OffsetDateTime.now(ZoneOffset.UTC)) < runTimeInSeconds) {
    Await.result(read(), Duration.Inf)
      .foreach(out => println("Result: " + out))
  }

  close()
}
