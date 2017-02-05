package wordCount;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;

/**
 * Created by cloudera on 1/12/17.
 */
public class Main {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("wordCountApp");
        sparkConf.setMaster("local[2]");

        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        ssc.checkpoint(".");

        // Initial state RDD input to mapWithState
        @SuppressWarnings("unchecked")
        List<Tuple2<String, CustomState>> tuples =
                Arrays.asList(new Tuple2<>("hello", new CustomState(1, 1)),
                        new Tuple2<>("world", new CustomState(1, 1)));

        JavaPairRDD<String, CustomState> initialRDD = ssc.sparkContext().parallelizePairs(tuples);

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
                "localhost", Integer.parseInt("9999"), StorageLevels.MEMORY_AND_DISK_SER_2);

        JavaDStream<String> words = lines.flatMap(WordsUtil::getWords);

        JavaPairDStream<String, Integer> wordsDstream = words.mapToPair(
                s -> new Tuple2<>(s, 1));

        // Update the cumulative count function
        Function3<String, Optional<Integer>, State<CustomState>, Tuple2<String, CustomState>> mappingFunc =
                (word, one, state) -> {
                    //int sum = one.orElse(0) + (state.exists() ? state.get().count : 0);
                    CustomState customState = new CustomState();
                    customState.count = one.orElse(0) + (state.exists() ? state.get().count : 0);
                    customState.count5 = one.orElse(1) * 5 + (state.exists() ? state.get().count5 : 0);

                    if (state.exists() && state.get().count == 5) {
                        state.remove();
                        System.out.println("state removed");
                        return new Tuple2<>(word, customState);

                    } else {
                        Tuple2<String, CustomState> output = new Tuple2<>(word, customState);

                        state.update(customState);
                        System.out.println("state updated");
                        System.out.println(word + ", count: " + customState.count + ", count5: " + customState.count5);

                        return output;
                    }
                };


        // DStream made of get cumulative counts that get updated in every batch
        JavaMapWithStateDStream<String, Integer, CustomState, Tuple2<String, CustomState>> stateDstream =
                wordsDstream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD));

        stateDstream.print();
        ssc.start();
        ssc.awaitTermination();
    }
}


