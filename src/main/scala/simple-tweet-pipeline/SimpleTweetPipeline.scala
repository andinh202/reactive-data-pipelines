import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.react.kafka.KafkaMessages._
import org.apache.kafka.common.serialization.{StringSerializer, StringDeserializer}
import com.softwaremill.react.kafka.{ProducerMessage, ConsumerProperties, ProducerProperties, ReactiveKafka}
import org.reactivestreams.{ Publisher, Subscriber }

/*
  =============================
  PIPELINE DEMO OVERVIEW
    1. Pull messages from Kafka Consumer into Akka ActorPublisher
    2. Push Messages through an Akka Stream/Runnable Flow and undergo some transformation (Source)
    3. Subscriber needs to read the messages from the Akka Stream/Runnable Flow (Sink)
    4. Subscriber/Sink dumps the transformed to the console
  =============================
  =============================
  OPTIONS FOR IMPLEMENTATION
    FLOW: ActorPublisher(Source) ---> Stream ---> ActorSubscriber(Sink)
    FLOW: ActorPublisher(Source) ---> Stream ---> Sink.actorRef(Sink)  *** IN USE BELOW
    FLOW: Source.actorRef(Source) ---> Stream ---> Sink.actorRef(Sink) // Built in simple source and sink
    FLOW: Source.actorRef(Source) ---> Stream ---> ActorSubscriber(Sink)
  =============================
*/
object SimpleTweetPipeline extends App {
  implicit val system = ActorSystem("ReactiveSimpleTweetPipeline")
  implicit val materializer = ActorMaterializer()

  val kafka = new ReactiveKafka()

  // Source is ActorPublisher, which must use StringConsumerRecord
  val simpleTweetPublisher: Publisher[StringConsumerRecord] = kafka.consume(ConsumerProperties(
    bootstrapServers = "localhost:9092",
    topic = "reactive-simple-tweet-pipeline",
    groupId = "reactive-simple-tweet-consumer",
    valueDeserializer = new StringDeserializer()
  ))

  // Sink is ActorSubscriber, which must use StringProducerMessage
  val simpleTweetSubscriber: Subscriber[StringProducerMessage] = kafka.publish(ProducerProperties(
    bootstrapServers = "localhost:9092",
    topic = "reactive-simple-tweet-pipeline",
    valueSerializer = new StringSerializer()
  ))

  Source.fromPublisher(simpleTweetPublisher)
    .map(m => m.value().toUpperCase) // make raw data upper-cased
    .map(text => SimpleTweet(text)) // convert to SimpleTweet object
    .map(st => ProducerMessage(st.text)) // convert back to ProducerMessage
    .to(Sink.fromSubscriber(simpleTweetSubscriber))
    .run()
}
