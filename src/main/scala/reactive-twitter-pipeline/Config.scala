import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.BasicClient

import scala.collection.JavaConversions._

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.{Constants, HttpHosts}
import com.twitter.hbc.core.event.Event
import com.twitter.hbc.httpclient.auth.OAuth1

object Config {
  // Twitter authentication credentials
  val CONSUMER_KEY = "Fn2GkcTo7MTXBUTH86gCcTCIg"
  val CONSUMER_SECRET = "UHcvIxWHjQl7M3VOvqQTNnRL3YAAmdTlFw9XL40vWl3waoPkOf"
  val ACCESS_TOKEN = "4870020185-ebPfDGBbjSTBX6aSkV11u9uuqokjRG9rAAi7LEv"
  val SECRET_TOKEN = "nHyoABn6hO1PJc7JgAmc9IRd3m9vD8Kzsd3hQ7eaVIJ4S"

  // Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
  val msgQueue = new LinkedBlockingQueue[String](100000)
  val eventQueue = new LinkedBlockingQueue[Event](1000)

  /*
    Handles all the Twitter HBC config and setup
   */
  def twitterHBCSetup: BasicClient = {
    // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
    val hosebirdHosts = new HttpHosts(Constants.STREAM_HOST)
    val hosebirdEndpoint = new StatusesFilterEndpoint()

    // Filter out tweets
    // Specifying the content of the data running through the pipeline
    // Mailchimp matches mailchimp, MAILCHIMP, #mailchimp, @mailchimp, etc... BUT NOT 'MailChimp'
    val terms = List("MailChimp", "Mailchimp", "MailChimp Status", "Mailchimp Status", "MailChimp UX", "Mailchimp UX", "MailChimp Design",
      "Mailchimp Design", "MailChimp API", "Mailchimp API", "Mandrill", "mandrillapp", "TinyLetter", "Tinyletter")
    hosebirdEndpoint.trackTerms(terms)

    // Pass in Auth for HBC Stream
    val hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, SECRET_TOKEN)

    // Setting up HBC client builder
    val hosebirdClient = new ClientBuilder()
      .name("Hosebird-Client-Reactive-Twitter-Pipeline")
      .hosts(hosebirdHosts)
      .authentication(hosebirdAuth)
      .endpoint(hosebirdEndpoint)
      .processor(new StringDelimitedProcessor(msgQueue))
      .eventMessageQueue(eventQueue)
      .build()

    hosebirdClient
  }
}
