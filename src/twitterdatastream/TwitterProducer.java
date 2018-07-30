package twitterdatastream;

import java.util.*;
import java.util.concurrent.*;
import jersey.repackaged.com.google.common.collect.Lists;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.Producer;
import com.twitter.hbc.*;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.httpclient.auth.*;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;

public class TwitterProducer {
	//public static void tweet() throws InterruptedException {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<String,String>(props);

		String Consumerkey = "n1Ryr4Opyetq9DYc3nPPgv1tu";
		String ConsumerSecret = "xkXPgc8KmZJVd0tQqKxPbvRRUt5RpeqLumLNBwPrvqpuPdHpeF";
		String AccessToken = "1075166491-E0tHEszU0m1lFA1lp1pf1Xazg3YBu8RAYcZSFvu";
		String SecretToken = "Sd4s1dpeE3y13aEctXTuIupl8TsGR3yMlzAWQMDw70Qhw";

		ProducerRecord<String, String> message = null;
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		endpoint.trackTerms(Lists.newArrayList("twitterapi", "#canada"));

		Authentication auth = new OAuth1(Consumerkey, ConsumerSecret, AccessToken, SecretToken);

		Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
				.endpoint(endpoint)
				.authentication(auth)
				.processor(new StringDelimitedProcessor(queue))
				.build();

		client.connect();

		for(int msgread = 0; msgread<1000; msgread++) {
			try {
				String msg = queue.take();
				System.out.println(msg);
				message = new ProducerRecord<String, String>("twitterstream",queue.take());
			}catch(InterruptedException e){
				e.printStackTrace();
			}
			producer.send(message);
			producer.flush();
			producer.close();
		}	
	}
}
/*		
		String Consumerkey = "n1Ryr4Opyetq9DYc3nPPgv1tu";
		String ConsumerSecret = "xkXPgc8KmZJVd0tQqKxPbvRRUt5RpeqLumLNBwPrvqpuPdHpeF";
		String AccessToken = "1075166491-E0tHEszU0m1lFA1lp1pf1Xazg3YBu8RAYcZSFvu";
		String SecretToken = "Sd4s1dpeE3y13aEctXTuIupl8TsGR3yMlzAWQMDw70Qhw";

		ProducerRecord<String, String> message = null;
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		endpoint.trackTerms(Lists.newArrayList("twitterapi", "#canada"));

		Authentication auth = new OAuth1(Consumerkey, ConsumerSecret, AccessToken, SecretToken);

		Client client = (Client) new ClientBuilder().hosts(Constants.STREAM_HOST)
				.endpoint(endpoint)
				.authentication(auth)
				.processor(new StringDelimitedProcessor(queue))
				.build();

		client.connect();

		for(int msgread = 0; msgread<1000; msgread++) {
			try {
				String msg = queue.take();
				System.out.println(msg);
				message = new ProducerRecord<String, String>("twitterstream",queue.take());
			}catch(InterruptedException e){
				e.printStackTrace();
			}
			producer.send(message);
			producer.flush();
			producer.close();
		}

	}
	public static void main(String[] args) {

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		//props.put("serializer.class", "kafka.serializer.StringEncoder");
		//ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new KafkaProducer<String,String>(props);

		try {
			TwitterProducer.tweet(producer);
		}catch(InterruptedException e) {
			System.out.println(e);
		}
	}*/
//}

