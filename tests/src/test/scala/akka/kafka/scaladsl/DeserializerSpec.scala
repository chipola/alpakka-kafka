/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.actor.ActorSystem
import akka.kafka.testkit.ConsumerResultFactory
import akka.kafka.tests.scaladsl.LogCapturing
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Broadcast, Sink, Source}
import akka.testkit.TestKit
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer, StringSerializer}
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{Matchers, TryValues, WordSpecLike}

class DeserializerSpec
    extends TestKit(ActorSystem("Spec"))
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with TryValues
    with IntegrationPatience
    with LogCapturing {

  implicit val mat = ActorMaterializer()

  val throwingDeserializer = new Deserializer[String] {
    override def deserialize(topic: String, data: Array[Byte]): String = {
      throw new SerializationException()
    }
  }

  "Plain Deserializer" should {

    "do things" in {

      val serializer = new StringSerializer
      val deserializer = new StringDeserializer
      val in =
        new ConsumerRecord("topic", 1, 1, serializer.serialize("topic", "key"), serializer.serialize("topic", "value"))

      val output = Source
        .single(in)
        .via(Deserializer.plain(deserializer, deserializer))
        .runWith(Sink.head)
        .futureValue
        .success
        .value

      output.key() should ===("key")
      output.value() should ===("value")
    }

    "not do things" in {
      val serializer = new StringSerializer
      val deserializer = throwingDeserializer
      val in =
        new ConsumerRecord("topic", 1, 1, serializer.serialize("topic", "key"), serializer.serialize("topic", "value"))

      val output = Source
        .single(in)
        .via(Deserializer.plain(deserializer, deserializer))
        .runWith(Sink.head)
        .futureValue
        .failure

      output.exception shouldBe a[SerializationException]
    }
  }

  "Committable Deserializer" should {

    "do things" in {

      val serializer = new StringSerializer
      val deserializer = new StringDeserializer
      val in = ConsumerResultFactory.committableMessage(
        record = new ConsumerRecord("topic",
                                    1,
                                    1,
                                    serializer.serialize("topic", "key"),
                                    serializer.serialize("topic", "value")),
        committableOffset = ConsumerResultFactory.committableOffset("group", "topic", 1, 1, "")
      )

      val output = Source
        .single(in)
        .via(Deserializer.committable(deserializer, deserializer))
        .runWith(Sink.head)
        .futureValue
        .success
        .value

      output.record.key() should ===("key")
      output.record.value() should ===("value")
    }

    "not do things" in {

      val serializer = new StringSerializer
      val deserializer = throwingDeserializer
      val in = ConsumerResultFactory.committableMessage(
        record = new ConsumerRecord("topic",
                                    1,
                                    1,
                                    serializer.serialize("topic", "key"),
                                    serializer.serialize("topic", "value")),
        committableOffset = ConsumerResultFactory.committableOffset("group", "topic", 1, 1, "")
      )

      val output = Source
        .single(in)
        .via(Deserializer.committable(deserializer, deserializer))
        .runWith(Sink.head)
        .futureValue
        .failure

      output.exception shouldBe a[SerializationException]
    }
  }
}
