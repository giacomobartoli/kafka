import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import twitter4j.*;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * A Kafka Producer that gets tweets on certain keywords
 * from twitter datasource and publishes to a kafka topic
 *
 * Please configure Configuration.java with API keys and access tokens.
 *
 */



public class KafkaTwitterProducer  {


    public static void main(String[] args) throws Exception {

        final LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);


        String[] parameters = {Configuration.consumerKey,Configuration.consumerSecret,Configuration.accessToken,Configuration.accessTokenSecret,Configuration.topicName,Configuration.hashtag};
        String[] keyWords = Arrays.copyOfRange(parameters, 5, parameters.length);

        // Set twitter oAuth tokens in the configuration
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true).setOAuthConsumerKey(Configuration.consumerKey).setOAuthConsumerSecret(Configuration.consumerSecret)
                .setOAuthAccessToken(Configuration.accessToken).setOAuthAccessTokenSecret(Configuration.accessTokenSecret);

        // Create twitterstream using the configuration
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        StatusListener listener = new StatusListener() {

            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + "upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                System.out.println("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        twitterStream.addListener(listener);

        // Filter keywords
        FilterQuery query = new FilterQuery().track(keyWords);
        twitterStream.filter(query);

         Thread.sleep(2000);

        // Add Kafka producer config settings
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        int i = 0;
        int j = 0;

        // poll for new tweets in the queue. If new tweets are added, send them
        // to the topic
        while (true) {
            Status ret = queue.poll();

            if (ret == null) {
                Thread.sleep(100);
                // i++;
            } else {
                for (HashtagEntity hashtage : ret.getHashtagEntities()) {
                    System.out.println("Tweet:" + ret);
                    System.out.println("Hashtag: " + hashtage.getText());
                    // producer.send(new ProducerRecord<String, String>(
                    // topicName, Integer.toString(j++), hashtage.getText()));
                    ProducerRecord r = new ProducerRecord<String, String>(Configuration.topicName, Integer.toString(j++), ret.getText());
                    producer.send(r);

                    // Use this log to see record structure in the kafka topic
                    //System.out.println("TopicName: "+Configuration.topicName+" Int: "+j+" Record: "+ret.getText());
                }
            }
        }
        // producer.close();
        // Thread.sleep(500);
        // twitterStream.shutdown();
    }


}
