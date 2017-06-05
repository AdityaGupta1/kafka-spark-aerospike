import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("KafkaSparkAerospike");
        printWithLines("created SparkConf", 2);
        // Durations.seconds(1) = 1 second batch interval
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        printWithLines("created JavaStreamingContext", 2);
        JavaSparkContext sc = jssc.sparkContext();
        printWithLines("created JavaSparkContext", 2);

        Map<String, Integer> topics = new HashMap<>();
        topics.put("test", 1);

        printWithLines("created topics HashMap", 2);

        JavaPairReceiverInputDStream<String, String> kafkaStream =
                KafkaUtils.createStream(jssc,
                        "localhost:2181,localhost:9092", "idk", topics);

        printWithLines("created JavaPairReceiverInputDStream", 2);

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