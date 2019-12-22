/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.NotUsed
import akka.kafka.ConsumerMessage.{CommittableMessage, TransactionalMessage}
import akka.stream.scaladsl.Flow
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.Deserializer

import scala.util.Try
import scala.languageFeature.higherKinds

object Deserializer {

  private def copyConsumerRecord[K, V](proto: ConsumerRecord[_, _], k: K, v: V): ConsumerRecord[K, V] = {
    new ConsumerRecord[K, V](
      proto.topic(),
      proto.partition(),
      proto.offset(),
      proto.timestamp(),
      proto.timestampType(),
      proto.checksum(),
      proto.serializedKeySize(),
      proto.serializedValueSize(),
      k,
      v,
      proto.headers(),
      proto.leaderEpoch()
    )
  }

  trait Extractor[M[_, _]] extends (M[Array[Byte], Array[Byte]] => ConsumerRecord[Array[Byte], Array[Byte]])

  trait Factory[M[_, _], K, V] extends ((M[Array[Byte], Array[Byte]], K, V) => M[K, V])

  private implicit val plainExtractor: Extractor[ConsumerRecord] =
    identity[ConsumerRecord[Array[Byte], Array[Byte]]]

  private implicit val committableExtractor: Extractor[CommittableMessage] = _.record

  private implicit val transactionalExtractor: Extractor[TransactionalMessage] = _.record

  private implicit def plainFactory[K, V]: Factory[ConsumerRecord, K, V] = copyConsumerRecord(_, _, _)

  private implicit def committableFactory[K, V]: Factory[CommittableMessage, K, V] =
    (p, k, v) => p.copy(record = copyConsumerRecord(p.record, k, v))

  private implicit def transactionalFactory[K, V]: Factory[TransactionalMessage, K, V] =
    (p, k, v) => p.copy(record = copyConsumerRecord(p.record, k, v))

  private def recordDeserializerFlow[M[_, _], K, V](
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V]
  )(
      implicit extractor: Extractor[M],
      factory: Factory[M, K, V]
  ): Flow[M[Array[Byte], Array[Byte]], Try[M[K, V]], NotUsed] = {
    Flow[M[Array[Byte], Array[Byte]]].map { x =>
      val result = Try {
        val key = keyDeserializer.deserialize(extractor(x).topic(), extractor(x).headers(), extractor(x).key())
        val value = valueDeserializer.deserialize(extractor(x).topic(), extractor(x).headers(), extractor(x).key())
        (key, value)
      }
      result.map(r => factory(x, r._1, r._2))
    }
  }

  def plain[K, V](
                   keyDeserializer: Deserializer[K],
                   valueDeserializer: Deserializer[V]
                 ): Flow[ConsumerRecord[Array[Byte], Array[Byte]], Try[ConsumerRecord[K, V]], NotUsed] = {
    recordDeserializerFlow(keyDeserializer, valueDeserializer)
  }

  def committable[K, V](
                         keyDeserializer: Deserializer[K],
                         valueDeserializer: Deserializer[V]
                       ): Flow[CommittableMessage[Array[Byte], Array[Byte]], Try[CommittableMessage[K, V]], NotUsed] = {
    recordDeserializerFlow(keyDeserializer, valueDeserializer)
  }

  def transactional[K, V](
                           keyDeserializer: Deserializer[K],
                           valueDeserializer: Deserializer[V]
                         ): Flow[TransactionalMessage[Array[Byte], Array[Byte]], Try[TransactionalMessage[K, V]], NotUsed] = {
    recordDeserializerFlow(keyDeserializer, valueDeserializer)
  }

}
