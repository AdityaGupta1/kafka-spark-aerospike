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

        String zkQuorum;

        if (args.length < 1) {
            printWithLines("Missing zkQuorum! Using default of \"localhost:9092,localhost:2181\". To specify a zkQuorum, give it as the first argument to the program.", 5);
            zkQuorum = "localhost:9092,localhost:2181";
        } else {
            // any characters, then a colon (:), then any digits
            Pattern addressPattern = Pattern.compile(".+:\\d+");

            // ""
            zkQuorum = args[0];
            for (String address : zkQuorum.split(",")) {
                if (!addressPattern.matcher(address).matches()) {
                    printWithLines("zkQuorum format is incorrect! Please specify it as a comma-separated list of IP addressed and try again.", 5);
                    return;
                }
            }
        }

        SparkConf conf = new SparkConf().setAppName("KafkaSparkAerospike");
        printWithLines("created SparkConf", 2);
        // Durations.seconds(1) = 1 second batch interval
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        printWithLines("created JavaStreamingContext", 2);
        printWithLines("created JavaSparkContext", 2);

        Map<String, Integer> topics = new HashMap<>();
        topics.put("test", 1);

        printWithLines("created topics HashMap", 2);



        JavaPairReceiverInputDStream<String, String> kafkaStream =
                KafkaUtils.createStream(jssc,
                        zkQuorum, "kafka-spark-aerospike", topics);

        printWithLines("created JavaPairReceiverInputDStream", 2);

        // set operation to run on each RDD
        kafkaStream.foreachRDD(rdd -> {
            if(!rdd.isEmpty()){
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