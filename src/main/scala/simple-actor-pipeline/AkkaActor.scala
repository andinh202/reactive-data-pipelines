import akka.actor.{Props, Actor}
import com.softwaremill.react.kafka.{ConsumerProperties, ReactiveKafka}
import org.apache.kafka.common.serialization.StringDeserializer

/*
  Actor that reads uses Kafka Consumer to read data from Kafka Topic
*/
class SimpleConsumerActor extends Actor {
  val processor = context.system.actorOf(Props[SimpleProcessorActor], "SimpleProcessor")

  override def preStart = {
    val kafka = new ReactiveKafka()

    context.actorOf(kafka.consumerActorProps(ConsumerProperties(
      bootstrapServers = "localhost:9092",
      topic = "reactive-simple-actor-pipeline",
      groupId = "reactive-simple-actor-consumer",
      valueDeserializer = new StringDeserializer()
    )))
  }

  def receive = {
    case msg: SimpleMessage => processor ! msg
  }
}

/*
  Receives SimpleMessage from SimpleConsumerActor and capitalizes it
*/
class SimpleProcessorActor extends Actor {
  val printer = context.system.actorOf(Props[SimplePrinterActor], "SimplePrinter")

  def receive = {
    case SimpleMessage(msg) => printer ! SimpleMessage(processMessage(msg)) // Send the current greeting back to the sender
  }

  def processMessage(message: String) = {
    message.toUpperCase
  }
}

/*
  Receives SimpleMessage from SimpleProcessorActor and dumps to console
*/
class SimplePrinterActor extends Actor {
  def receive = {
    case SimpleMessage(message) => println(message)
  }
}