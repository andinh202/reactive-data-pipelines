import java.util.Properties

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.ActorMaterializer
import com.softwaremill.react.kafka.{ConsumerProperties, ReactiveKafka}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConversions._

/*
  =============================
   1. Publish messages to Kafka Topic through command-line Producer
   2. Push messages to an Akka Actor from the Kafka Consumer (which polls from Kafka Topic)
   3. Messages go through a actor transition and undergo transformation
   4. Final actor dumps output to console
   Actor 1 ----> Actor 2 -----> Actor 3 -----> Dumps to console
  =============================
*/
object SimpleActorPipeline extends App {
  // Akka Steam actor setup and creation
  implicit val materializer = ActorMaterializer()
  implicit val system = ActorSystem("ReactiveSimpleActorPipeline")

  val kafka = new ReactiveKafka()

  // consumer
  val consumerProperties = ConsumerProperties(
    bootstrapServers = "localhost:9092",
    topic = "reactive-simple-actor-pipeline",
    groupId = "reactive-simple-actor-consumer",
    valueDeserializer = new StringDeserializer()
  )

  val consumerActorProps: Props = kafka.consumerActorProps(consumerProperties)
  val publisherActor: ActorRef = system.actorOf(consumerActorProps)
  //publisherActor.tell(msg) /*What do I DO????




  /**
  val simpleActor = system.actorOf(Props[SimpleActor], "SimpleActor")
  val POLL_TIME = 100 // time in ms

  // Props for Kafka Consumer
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("group.id", "test")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  props.put("session.timeout.ms", "30000")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(List("simple-actor-pipeline")) // Kafka-Consumer reading from the topic

  println("simple-actor data-pipeline starting...")
  while (true) {
    val records = consumer.poll(POLL_TIME) // Kafka-Consumer data collection
    for (record <- records) { // Kafka-Consumer data message
      simpleActor.tell(SimpleMessage(record.value), ActorRef.noSender)
    }
  }
    */
}