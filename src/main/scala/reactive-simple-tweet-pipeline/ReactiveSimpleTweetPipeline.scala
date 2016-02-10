import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.react.kafka.KafkaMessages._
import org.apache.kafka.common.serialization.{StringSerializer, StringDeserializer}
import com.softwaremill.react.kafka.{ProducerMessage, ConsumerProperties, ProducerProperties, ReactiveKafka}
import org.reactivestreams.{ Publisher, Subscriber }

/*
  =============================
  REACTIVE PIPELINE DEMO OVERVIEW
    1. Use the Kafka Producer through the command line to send messages to the lowercase Kafka Topic
    2. ActorPublisher uses Kafka Consumer to read from Topic and push to First Stream
    3. First Stream capitalizes messages and transforms to StringProducerMessage
    4. ActorSubscriber reads from stream and uses Kafka Producer to push to uppercase Kafka Topic
    5. ActorPublisher uses Kafka Consumer to read from uppercase Kafka Topic and push to Second Stream
    6. Second Stream transforms from StringConsumerRecord to SimpleTweet
    7. Console Sink reads from Second Stream and dumps output to console
  Kafka Topic --> ActorPub --> First Stream --> ActorSub --> Kafka Topic --> ActorPub --> Second Stream --> Console Sink
  =============================
*/
object ReactiveSimpleTweetPipeline extends App {
  implicit val system = ActorSystem("ReactiveSimpleTweetPipeline")
  implicit val materializer = ActorMaterializer()

  // Kafka Topics and Server
  val LC_TOPIC = "reactive-simple-tweet-lowercase"
  val UC_TOPIC = "reactive-simple-tweet-uppercase"
  val LC_GROUP_ID = "reactive-simple-tweet-lc-consumer"
  val UC_GROUP_ID = "reactive-simple-tweet-uc-consumer"
  val SERVER = "localhost:9092"

  // instantiate reactive kafka
  val kafka = new ReactiveKafka()

  // 1 : done on command line through kakfa producer

  // 2
  // Source of Stream is ActorPublisher, which must use StringConsumerRecord
  // ActorPublisher encapsulates Kafka Consumer to read from topic
  val lowerCasePublisher: Publisher[StringConsumerRecord] = kafka.consume(ConsumerProperties(
    bootstrapServers = SERVER,
    topic = LC_TOPIC, // Kafka Topic read by consumer
    groupId = LC_GROUP_ID,
    valueDeserializer = new StringDeserializer()
  ))

  // 4
  // Sink of Stream is ActorSubscriber, which must use StringProducerMessage
  // ActorSubscriber encapsulates Kafka Producer which publishes to topic
  val upperCaseSubscriber: Subscriber[StringProducerMessage] = kafka.publish(ProducerProperties(
    bootstrapServers = SERVER,
    topic = UC_TOPIC, // Kafka Topic published by producer
    valueSerializer = new StringSerializer()
  ))

  // 3
  // First Stream which transforms lowercase SimpleTweets to uppercase
  // Source: lowerCasePublisher  Sink: upperCaseSubscriber
  val firstStream = Source.fromPublisher(lowerCasePublisher)
    .map(m => m.value().toUpperCase) // make raw data upper-cased
    .map(m => ProducerMessage(m)) // convert to ProducerMessage for ActorSubscriber
    .to(Sink.fromSubscriber(upperCaseSubscriber))
  firstStream.run()

  // 5
  // Source is ActorPublisher, which must use StringConsumerRecord
  // ActorPublisher encapsulates Kafka Consumer to read from topic
  val upperCasePublisher: Publisher[StringConsumerRecord] = kafka.consume(ConsumerProperties(
    bootstrapServers = SERVER,
    topic = UC_TOPIC, // Kafka Topic read by consumer
    groupId = UC_GROUP_ID,
    valueDeserializer = new StringDeserializer()
  ))

  // 7
  // Sink of Stream that dumps to console, no ActorSubscriber
  val consoleSink = Sink.foreach[Tweet](tweet => {
    println("CONSOLE SINK: " + tweet.text)
    Thread.sleep(1000) // simulate how akka-streams handles Backpressure
  })

  // 6
  // Final Stream which converts from StringConsumerRecord to SimpleTweet
  // Source: upperCasePublisher  Sink: consoleSink
  val secondStream = Source.fromPublisher(upperCasePublisher)
    .map(m => SimpleTweet(m.value())) // convert to SimpleTweet object
    .to(consoleSink)
  secondStream.run()

}
