import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;



public class MultipleConsumer{

    private static String groupId = "tweetsConsumer";
    private static int N_CONSUMERS = 3;


    public static void main(String[] args) {
        List<String> topics = Arrays.asList("tweets2");
        final ExecutorService executor = Executors.newFixedThreadPool(N_CONSUMERS);

        final List<ConsumerLoop> consumers = new ArrayList();
        for (int i = 0; i < N_CONSUMERS; i++) {
            ConsumerLoop consumer = new ConsumerLoop(i, groupId, topics);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (ConsumerLoop consumer : consumers) {
                    consumer.shutdown();
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

}
