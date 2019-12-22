/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl

import akka.NotUsed
import akka.kafka.ConsumerMessage.{CommittableMessage, TransactionalMessage}
import akka.stream.javadsl.Flow
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Deserializer

import scala.util.Try

object Deserializer {

  def plain[K, V](
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V]
  ): Flow[ConsumerRecord[Array[Byte], Array[Byte]], Try[ConsumerRecord[K, V]], NotUsed] =
    akka.kafka.scaladsl.Deserializer.plain(keyDeserializer, valueDeserializer).asJava

  def committable[K, V](
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V]
  ): Flow[CommittableMessage[Array[Byte], Array[Byte]], Try[CommittableMessage[K, V]], NotUsed] =
    akka.kafka.scaladsl.Deserializer.committable(keyDeserializer, valueDeserializer).asJava

  def transactional[K, V](
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V]
  ): Flow[TransactionalMessage[Array[Byte], Array[Byte]], Try[TransactionalMessage[K, V]], NotUsed] =
    akka.kafka.scaladsl.Deserializer.transactional(keyDeserializer, valueDeserializer).asJava

}
