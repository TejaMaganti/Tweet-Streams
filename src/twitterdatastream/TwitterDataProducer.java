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

public class TwitterDataProducer {
	private static final String topic = "twitterstream";
	private static final String consumerkey = "";
	private static final String consumersecret = "";
	private static final String accesstoken = "";
	private static final String accesssecret = "";
	
	public static void sendmessage() throws InterruptedException {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		
		BlockingQueue<String> blockqueue = new LinkedBlockingQueue<String>(10000);
		
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
		System.out.println("Client Started");
		
		
		for(int i = 0; i<1000; i++) {
			ProducerRecord <String, String> record = null;
			try {
				record = new ProducerRecord<String, String>(topic, blockqueue.take());
			}catch(InterruptedException e){
				e.printStackTrace();
			}
			producer.send(record);
		}
		producer.flush();
		producer.close();
		client.stop();
	}
	
	public static void main(String[] args) {
		try {
			TwitterDataProducer.sendmessage();
		}catch (InterruptedException e) {
			System.out.println(e);
		}
	}
}
