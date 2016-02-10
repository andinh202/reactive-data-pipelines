import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.react.kafka.KafkaMessages.StringConsumerRecord
import com.softwaremill.react.kafka.{ConsumerProperties, ReactiveKafka}
import org.apache.kafka.common.serialization.StringDeserializer
import org.reactivestreams.Publisher

/*
  =============================
  REACTIVE SIMPLE ACTOR PIPELINE OVERVIEW
   1. Publish messages to Kafka Topic through command-line Producer
   2. Read messages using Kakfa Consumer encapsulated by ActorPublisher
   3. Messages go through stream where they are capitalized and stored as SimpleMessage
   4. Console Sink dumps SimpleMessage to console

  KafkaTopic --> ActorPub --> Stream --> ConsoleSink
  =============================
*/
object ReactiveSimpleActorPipeline extends App {
  implicit val system = ActorSystem("ReactiveSimpleActorPipeline")
  implicit val materializer = ActorMaterializer()

  val kafka = new ReactiveKafka()

  // 1 : done on command line through kakfa producer

  // 2
  // Source of Stream is ActorPublisher, which must use StringConsumerRecord
  // ActorPublisher encapsulates Kafka Consumer to read from topic
  val consumerActor: Publisher[StringConsumerRecord] = kafka.consume(ConsumerProperties(
    bootstrapServers = "localhost:9092",
    topic = "reactive-simple-actor-pipeline",
    groupId = "reactive-simple-actor-consumer",
    valueDeserializer = new StringDeserializer()
  ))

  // 4
  // Sink of Stream that dumps to console, no ActorSubscriber
  val consoleSink = Sink.foreach[SimpleMessage](sm => {
    println("CONSOLE SINK: " + sm.message)
  })

  // 3
  // Stream which transforms lowercase messages to uppercase SimpleMessages
  // Source: ConsumerActor  Sink: ConsoleSink
  val stream = Source.fromPublisher(consumerActor)
    .map(m => m.value()) // convert back to string message
    .map(msg => msg.toUpperCase) // capitalize the string message
    .map(msgUC => new SimpleMessage(msgUC)) // store capitalized string message as SimpleMessage
    .to(consoleSink)
  stream.run()

}