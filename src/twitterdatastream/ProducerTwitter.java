package twitterdatastream;


import java.util.*;
import java.util.concurrent.*;
import jersey.repackaged.com.google.common.collect.Lists;
import org.apache.kafka.clients.producer.*;
import com.twitter.hbc.*;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.httpclient.auth.*;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;

public class ProducerTwitter {
	private static final String topic = "twitterstream";
	private static final String consumerkey = "n1Ryr4Opyetq9DYc3nPPgv1tu";
	private static final String consumersecret = "xkXPgc8KmZJVd0tQqKxPbvRRUt5RpeqLumLNBwPrvqpuPdHpeF";
	private static final String accesstoken = "1075166491-E0tHEszU0m1lFA1lp1pf1Xazg3YBu8RAYcZSFvu";
	private static final String accesssecret = "Sd4s1dpeE3y13aEctXTuIupl8TsGR3yMlzAWQMDw70Qhw";
	
	public static void sendmessage() throws InterruptedException {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		
		BlockingQueue<String> blockqueue = new LinkedBlockingQueue<String>(10000);
		//BlockingQueue<Event> eventqueue = new LinkedBlockingQueue(1000);
		
		Hosts hbc = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		endpoint.trackTerms(Lists.newArrayList("twitterapi", "#yolo", "trump", "donald", "lol"));
		
		Authentication auth = new OAuth1(consumerkey,consumersecret,accesstoken,accesssecret);
		
		ClientBuilder builder = new ClientBuilder()
				.hosts(hbc)
				.authentication(auth)
				.endpoint(endpoint)
				.processor(new StringDelimitedProcessor(blockqueue));
		Client client = builder.build();
		
	
		client.connect();
		
		
		//Code Breaks here
		for(int i = 0; i<1000; i++) {
			ProducerRecord <String, String> record = null;
			try {
				record = new ProducerRecord<String, String>(topic, blockqueue.take());
			}catch(InterruptedException e){
				e.printStackTrace();
			}
			producer.send(record);
		}
/*		//blockqueue.put("hey");
		System.out.println("started");
		//while (!client.isDone())
		for(int i = 0; i<1000; i++){
			//System.out.println("in");
			  String msg = blockqueue.take();
			  //System.out.println("in 2");
			  System.out.println(msg);
			}*/
		System.out.println("end");
		producer.flush();
		producer.close();
		client.stop();
	}
	
	public static void main(String[] args) {
		try {
			ProducerTwitter.sendmessage();
		}catch (InterruptedException e) {
			System.out.println(e);
		}
	}
}
