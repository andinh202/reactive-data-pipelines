import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.react.kafka.KafkaMessages.StringConsumerRecord
import com.softwaremill.react.kafka.{ConsumerProperties, ProducerMessage, ProducerProperties, ReactiveKafka}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.reactivestreams.{Publisher, Subscriber}

/*
  =============================
  REACTIVE TWITTER PIPELINE OVERVIEW
    1. Pull raw json tweets from Twitter HBC
    2. Push raw json tweets into Kafka topic through Kafka Producer
    3. Pull raw json tweets from Kafka Consumer and store in Akka Publisher
    4. Akka Publisher sends raw json through first stream to transform/serialize to Tweet object
    5. Akka Subscriber takes serialized Tweet object and uses Kafka Producer to push to another Kafka Topic
    6. Kafka Consumer inside Akka Publisher pulls from topic and sends the serialized tweet through transformed stream
    7. Final Stream deserializes the Tweet object
    8. Sink reads from Final Stream and dumps to console

  TwitterHBC --> KafkaProd --> KafkaTopic --> ActorPub --> RawStream --> ActorSub --> KafkaTopic --> ActorPub -->
    TransformedStream --> ConsoleSink
  =============================
*/
object ReactiveTwitterPipeline extends App {
  implicit val system = ActorSystem("ReactiveTwitterPipeline")
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

  // 1
  // Reads data from Twitter HBC on own thread
  val hbcTwitterStream = new Thread {
    override def run() = {
      hosebirdClient.connect() // Establish a connection to Twitter HBC stream
      while (!hosebirdClient.isDone()) {
        val event = Config.eventQueue.take()
        val tweet = Config.msgQueue.take()

        // 2
        // kafka producer publish tweet to kafka topic
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

  // 3
  // Source in this example is an ActorPublisher publishing raw tweet json
  // keyDeserializer by default is ByteArrayDeSerializer
  val rawTweetPublisher: Publisher[StringConsumerRecord] = kafka.consume(ConsumerProperties(
    bootstrapServers = SERVER,
    topic = RAW_TOPIC,
    groupId = RAW_GROUP_ID,
    valueDeserializer = new StringDeserializer()
  ))

  // 5
  // ActorSubscriber is the sink that uses Kafka Producer to push back into Kafka Topic
  // keySerializer by default is ByteArraySerializer
  // ProducerMessage types must be declared if not using type alias StringProducerMessage
  val richTweetSubscriber: Subscriber[ProducerMessage[Array[Byte], Array[Byte]]] = kafka.publish(ProducerProperties(
    bootstrapServers = SERVER,
    topic = TOPIC,
    valueSerializer = new ByteArraySerializer()
  ))

  // 4
  // Akka Stream/Flow: ActorPublisher ---> raw JSON ---> Tweet ---> Array[Byte] ---> ProducerMessage ---> ActorSubscriber
  val rawStream = Source.fromPublisher(rawTweetPublisher)
    .map(m => parse(m.value()))
    .map(json => Util.extractTweetFields(json))
    .map(tweet => Util.serialize[Tweet](tweet))
    .map(bytes => ProducerMessage(bytes)) // convert to ProducerMessage for ActorSubscriber
    .to(Sink.fromSubscriber(richTweetSubscriber))
  rawStream.run()

  // 6
  // Source in this example  is an ActorPublisher publishing transformed json ---> tweet
  // keySerializer by default is ByteArraySerializer
  // ConsumerRecord types must be declared if not using type alias StringConsumerRecord
  val richTweetPublisher: Publisher[ConsumerRecord[Array[Byte], Array[Byte]]] = kafka.consume(ConsumerProperties(
    bootstrapServers = SERVER,
    topic = TOPIC,
    groupId = GROUP_ID,
    valueDeserializer = new ByteArrayDeserializer()
  ))

  // 8
  // Sink simply prints to the console
  val consoleSink = Sink.foreach[Tweet](tweet => {
    println("=========================================================================")
    println(tweet)
  })

  // 7
  // Akka Stream/Flow: ActorPublisher ---> ConsumerRecord ---> Array[Byte] ---> Tweet ---> ConsoleSink
  val transformedStream = Source.fromPublisher(richTweetPublisher)
    .map(record => record.value())
    .map(bytes => Util.deserialize[Tweet](bytes))
    .to(consoleSink)
  transformedStream.run()

}