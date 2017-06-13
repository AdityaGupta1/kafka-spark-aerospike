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

import java.util.*;
import java.util.regex.Pattern;

public class Main {
    private static String argDuration, argZk, argTopic, argAerospike;

    private static int batchDuration;
    private static int defaultBatchDuration = 1000;
    private static String zk;
    private static String defaultZk = "localhost:2181";
    private static String topic;
    private static String defaultTopic = "test";
    private static String[] aerospike;
    private static String[] defaultAerospike = {"localhost", "3000"};

    private static final Pattern ipPattern = Pattern.compile(".+:.+");

    private static AerospikeClient aerospikeClient = null;
    private static final Key key = new Key("test", "test", "test");
    private static Record record;

    public static void main(String[] args) throws InterruptedException {
        argDuration = argZk = argTopic = argAerospike = "default";

        setArgs(args);
        checkArgs();
        printArgs();

        aerospikeClient = new AerospikeClient(aerospike[0], Integer.parseInt(aerospike[1]));
        record = aerospikeClient.get(null, key);

        SparkConf conf = new SparkConf().setAppName("KafkaSparkAerospike");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.milliseconds(batchDuration));

        Map<String, Integer> topics = new HashMap<>();
        topics.put(argTopic, 1);


        JavaPairReceiverInputDStream<String, String> kafkaStream =
                KafkaUtils.createStream(jssc,
                        zk, "kafka-spark-aerospike", topics);

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

    private static boolean checkForBins() {
        try {
            record.bins.size();
        } catch (NullPointerException e) {
            return false;
        }

        return true;
    }

    private static int binCount = checkForBins() ? record.bins.size() : 0;

    private static void writeBin(String value) {
        binCount++;
        aerospikeClient.put(null, key, new Bin(Integer.toString(binCount - 1), value));
    }

    private static void readBins() {
        printWithLines(binCount, 10);

        if (!checkForBins() || binCount == 0) {
            return;
        }

        record = aerospikeClient.get(null, key);

        // key values in order
        List<String> keys = new ArrayList<>(record.bins.keySet());
        keys.sort((string1, string2) -> {
            int int1 = Integer.parseInt(string1);
            int int2 = Integer.parseInt(string2);
            return Integer.compare(int1, int2);
        });

        // bin values in order
        List<Object> values = new ArrayList<>();

        for (String bin : keys) {
            values.add(record.bins.get(bin));
        }

        printWithLines(keys + "\n" + values, 5);
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

    private static void errorWithLines(Object message, int lines) {
        printLines(lines);
        System.err.println(message);
        printLines(lines);
    }

    private static final String BATCH_DURATION_KEY = "-duration";
    private static final String ZK_KEY = "-zk";
    private static final String TOPIC_KEY = "-topic";
    private static final String AEROSPIKE_KEY = "-aerospike";

    private static void setArgs(String[] args) {
        for (int i = 0; i < args.length; i += 2) {
            String key = args[i];
            String value = args[i + 1];

            switch (key) {
                case BATCH_DURATION_KEY:
                    argDuration = value;
                    break;
                case ZK_KEY:
                    argZk = value;
                    break;
                case TOPIC_KEY:
                    argTopic = value;
                    break;
                case AEROSPIKE_KEY:
                    argAerospike = value;
                    break;
            }
        }
    }

    private static void checkArgs() {
        // batch duration
        if (argDuration.equals("default")) {
            batchDuration = defaultBatchDuration;
        } else {
            try {
                batchDuration = Integer.parseInt(argDuration);
            } catch (Exception e) {
                errorWithLines("Invalid batch duration! Using default of " + defaultBatchDuration + " (milliseconds).", 5);
                batchDuration = defaultBatchDuration;
            }
        }

        // zkQuorum
        if (argZk.equals("default")) {
            zk = defaultZk;
        } else {
            boolean validZk = true;
            for (String ip : argZk.split(",")) {
                if (!ipPattern.matcher(ip).matches()) {
                    validZk = false;
                }
            }

            if (!validZk) {
                errorWithLines("Invalid ZooKeeper IP addresses! Please specify them as a comma-separated list of addresses. Using default of \"" + defaultZk + "\".", 5);
                zk = defaultZk;
            } else {
                zk = argZk;
            }
        }

        // topic
        if (argTopic.equals("default")) {
            topic = defaultTopic;
        } else {
            topic = argTopic;
        }

        // Aerospike
        if (argAerospike.equals("default")) {
            aerospike = defaultAerospike;
        } else {
            if (!ipPattern.matcher(argAerospike).matches()) {
                errorWithLines("Invalid Aerospike IP addresses! Using default of \"" + defaultAerospike[0] + ":" + defaultAerospike[1] + "\".", 5);
                aerospike = defaultAerospike;
            } else {
                aerospike = argAerospike.split(":");
            }
        }
    }

    private static void printArgs() {
        printWithLines("Batch duration: " + batchDuration + " milliseconds\n" +
                "ZooKeeper IP addresses: \"" + zk + "\"\n" +
                "Topic: \"" + topic + "\"\n" +
                "Aerospike IP address: \"" + aerospike[0] + ":" + aerospike[1] + "\"", 5);
    }
}