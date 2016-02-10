import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.ActorMaterializer
import com.softwaremill.react.kafka.{ConsumerProperties, ReactiveKafka}
import org.apache.kafka.common.serialization.StringDeserializer

/*
  =============================
  REACTIVE SIMPLE ACTOR PIPELINE OVERVIEW
   1. Publish messages to Kafka Topic through command-line Producer
   2. Push messages to an Akka Actor from the Kafka Consumer (which polls from Kafka Topic)
   3. Messages go through an actor pipeline and undergo transformation
   4. Final actor dumps output to console

  KafkaTopic --> SimpleActor --> SimpleProcessor --> SimplePrinter --> Dumps to console
  =============================
*/
object SimpleActorPipeline extends App {
  implicit val system = ActorSystem("ReactiveSimpleActorPipeline")
  implicit val materializer = ActorMaterializer()

  val simpleConsumerActor = system.actorOf(Props[SimpleConsumerActor], "SimpleConsumerActor")
  println("simple-actor reactive-data-pipeline starting...")
  println("======================================")


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