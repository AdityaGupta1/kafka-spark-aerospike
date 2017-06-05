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
    public static void main(String[] args) throws InterruptedException {
        int batchDurationMilliseconds;

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

        String zkQuorum;

        if (args.length < 2) {
            printWithLines("Missing zkQuorum! Using default of \"localhost:9092,localhost:2181\". To specify a zkQuorum, give it as the second argument to the program.", 5);
            zkQuorum = "localhost:9092,localhost:2181";
        } else {
            // any characters, then a colon (:), then any digits
            Pattern addressPattern = Pattern.compile(".+:\\d+");

            zkQuorum = args[1];
            for (String address : zkQuorum.split(",")) {
                if (!addressPattern.matcher(address).matches()) {
                    printWithLines("zkQuorum format is incorrect! Please specify it as a comma-separated list of IP addresses and try again.", 5);
                    return;
                }
            }
        }

        printWithLines("Using zkQuorum of " + zkQuorum, 2);
        printWithLines("Using batch duration of " + batchDurationMilliseconds + " milliseconds", 2);

        SparkConf conf = new SparkConf().setAppName("KafkaSparkAerospike");
        // Durations.seconds(1) = 1 second batch interval
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.milliseconds(batchDurationMilliseconds));

        Map<String, Integer> topics = new HashMap<>();
        topics.put("test", 1);

        printWithLines("created topics HashMap", 2);


        JavaPairReceiverInputDStream<String, String> kafkaStream =
                KafkaUtils.createStream(jssc,
                        zkQuorum, "kafka-spark-aerospike", topics);

        // set operation to run on each RDD
        kafkaStream.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                List<Tuple2<String, String>> tuples = rdd.collect();
                for (Tuple2 tuple : tuples) {
                    printWithLines(tuple._2, 1);
                }
            }
        });

        jssc.start();

        jssc.awaitTermination();
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
}