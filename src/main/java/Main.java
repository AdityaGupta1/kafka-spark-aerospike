import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class Main {

    private static String topic;
    private static int batchDurationMilliseconds;
    private static String zkQuorum;
    private static final AerospikeClient aerospikeClient = new AerospikeClient("172.28.128.3", 3000);
    private static final Key key = new Key("test", "test", "test");

    public static void main(String[] args) throws InterruptedException {
        setArgs(args);

        SparkConf conf = new SparkConf().setAppName("KafkaSparkAerospike");
        // Durations.seconds(1) = 1 second batch interval
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.milliseconds(batchDurationMilliseconds));

        Map<String, Integer> topics = new HashMap<>();
        topics.put(topic, 1);

        printWithLines("created topics HashMap", 2);


        JavaPairReceiverInputDStream<String, String> kafkaStream =
                KafkaUtils.createStream(jssc,
                        zkQuorum, "kafka-spark-aerospike", topics);

        // set operation to run on each RDD
        kafkaStream.foreachRDD(rdd -> {
            // key = timestamp; value = value
            if (!rdd.isEmpty()) {
                List<Tuple2<String, String>> tuples = rdd.collect();
                for (Tuple2 tuple : tuples) {
                    writeBin(tuple._2.toString());
                }
            }

            readBins();
        });

        jssc.start();

        jssc.awaitTermination();
    }

    static int binCount = 0;

    private static void writeBin(String value) {
        binCount++;
        aerospikeClient.put(null, key, new Bin(Integer.toString(binCount), value));
    }

    private static void readBins() {
        Record record = aerospikeClient.get(null, key);
        try {
            printWithLines(record.bins.keySet() + "\n" + record.bins.values(), 5);
        } catch (NullPointerException e) {
            // do nothing
        }
    }

    private static void printLines(int lines) {
        for (int i = 0; i < lines; i++) {
            System.out.println();
        }
    }

    private static void printWithLines(Object message, int lines) {
        printLines(lines);
        System.out.println(message);
        printLines(lines);
    }

    private static void setArgs(String[] args) {
        if (args.length < 1) {
            printWithLines("Missing batch duration! Using default of \"1000\" (milliseconds). To specify a batch duration, give it (in milliseconds) as the first argument to the program.", 5);
            batchDurationMilliseconds = 1000;
        } else {
            try {
                batchDurationMilliseconds = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                printWithLines("Batch duration must be a positive integer less than 2,147,483,648! Please specify it as such and try again.", 5);
                return;
            }

            if (batchDurationMilliseconds < 0) {
                printWithLines("Batch duration must be a positive integer less than 2,147,483,648! Please specify it as such and try again.", 5);
                return;
            }
        }

        if (args.length < 2) {
            printWithLines("Missing zkQuorum! Using default of \"localhost:2181\". To specify a zkQuorum, give it as the second argument to the program.", 5);
            zkQuorum = "localhost:2181";
        } else {
            // any characters, then a colon (:), then any characters
            Pattern addressPattern = Pattern.compile(".+:.+");

            zkQuorum = args[1];
            for (String address : zkQuorum.split(",")) {
                if (!addressPattern.matcher(address).matches()) {
                    printWithLines("zkQuorum format is incorrect! Please specify it as a comma-separated list of IP addresses and try again.", 5);
                    return;
                }
            }
        }

        if (args.length < 3) {
            printWithLines("Missing topic! Using default of \"test\". To specify a topic, give it as the third argument to the program.", 5);
            topic = "test";
        } else {
            topic = args[2];
        }

        printWithLines("Using batch duration of " + batchDurationMilliseconds + " milliseconds", 2);
        printWithLines("Using zkQuorum of \"" + zkQuorum + "\"", 2);
        printWithLines("Using topic of \"" + topic + "\"", 2);
    }
}