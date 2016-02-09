import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.react.kafka.{ConsumerProperties, ProducerMessage, ProducerProperties, ReactiveKafka}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.reactivestreams.{Publisher, Subscriber}

/*
  =============================
  REACTIVE PIPELINE DEMO OVERVIEW
    1. Pull raw json tweets from Twitter HBC client
    2. Push raw json tweets into Kafka topic through Kafka Producer
    3. Pull raw json tweets from Kafka Consumer and store in Akka Publisher
    4. Akka Publisher sends raw json through first stream to transform/serialize to Tweet object
    5. Akka Subscriber takes serialized Tweet object and uses Kafka Producer to push to another Kafka Topic
    6. Kafka Consumer inside Akka Publisher pulls from topic and sends the serialized tweet through final stream
    7. Final Stream deserializes the Tweet object and dumps to console sink
  =============================
*/
object ReactiveTwitterPipeline extends App {
  // Akka Actor and Producer/Subscriber setup
  implicit val system = ActorSystem("TwitterPipeline")
  implicit val materializer = ActorMaterializer()

  // Kafka Topics and Server
  val RAW_TOPIC = "reactive-twitter-pipeline-raw"
  val TOPIC = "reactive-twitter-pipeline"
  val RAW_GROUP_ID = "reactive-twitter-pipeline-consumer-raw"
  val GROUP_ID = "reactive-twitter-pipeline-consumer"
  val SERVER = "localhost:9092"

  // instantiate reactive kafka
  val kafka = new ReactiveKafka()

  // Setting up HBC client builder
  val hosebirdClient = Config.twitterHBCSetup

  // Reads data from Twitter HBC on own thread
  val hbcTwitterStream = new Thread {
    override def run() = {
      hosebirdClient.connect() // Establish a connection to Twitter HBC stream
      while (!hosebirdClient.isDone()) {
        val event = Config.eventQueue.take()
        val tweet = Config.msgQueue.take()
        // kafka producer publish tweet to kafka topic
        //rawTwitterProducer.send(new ProducerRecord(RAW_TOPIC, tweet))
        kafka.publish(ProducerProperties(
          bootstrapServers = SERVER,
          topic = RAW_TOPIC,
          valueSerializer = new StringSerializer()
        )).onNext(ProducerMessage(tweet))
      }
      hosebirdClient.stop() // Closes connection with Twitter HBC stream
    }
  }

  println("twitter reactive-data-pipeline starting...")
  hbcTwitterStream.start() // Starts the thread which invokes run()

  // Source in this example is an ActorPublisher publishing raw tweet json
  val rawTweetPublisher: Publisher[ConsumerRecord[Array[Byte], String]] = kafka.consume(ConsumerProperties(
    bootstrapServers = SERVER,
    topic = RAW_TOPIC,
    groupId = RAW_GROUP_ID,
    valueDeserializer = new StringDeserializer()
  ))

  // ActorSubscriber is the sink that uses Kafka Producer to push back into Kafka Topic
  val richTweetSubscriber: Subscriber[ProducerMessage[Array[Byte], Array[Byte]]] = kafka.publish(ProducerProperties(
    bootstrapServers = SERVER,
    topic = TOPIC,
    valueSerializer = new ByteArraySerializer()
  ))

  // Akka Stream/Flow: ActorPublisher ---> raw JSON ---> Tweet ---> Array[Byte] ---> ActorSubscriber
  val rawStream = Source.fromPublisher(rawTweetPublisher)
    .map(m => parse(m.value()))
    .map(json => Util.extractTweetFields(json))
    .map(tweet => Util.serialize[Tweet](tweet))
    .map(m => ProducerMessage(m)) // convert to ProducerMessage for ActorSubscriber
    .to(Sink.fromSubscriber(richTweetSubscriber))
  rawStream.run()

  // Source in this example  is an ActorPublisher publishing transformed tweet json
  val richTweetPublisher: Publisher[ConsumerRecord[Array[Byte], Array[Byte]]] = kafka.consume(ConsumerProperties(
    bootstrapServers = SERVER,
    topic = TOPIC,
    groupId = GROUP_ID,
    valueDeserializer = new ByteArrayDeserializer()
  ))
  // Sink is simply the console
  val consoleSink = Sink.foreach[Tweet](tweet => {
    println("=========================================================================")
    println(tweet)
  })

  // Akka Stream/Flow: ActorPublisher ---> Array[Byte] ---> Tweet ---> ConsoleSink
  val transformedStream = Source.fromPublisher(richTweetPublisher)
    .map(bytes => Util.deserialize[Tweet](bytes.value()))
    .to(consoleSink)
  transformedStream.run()
}