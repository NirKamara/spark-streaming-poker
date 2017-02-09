package wordCount;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.In;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;

import com.google.common.base.Optional;

import javax.xml.bind.SchemaOutputResolver;


public class Main {

    private static  org.apache.log4j.Logger log = Logger.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        //AnnotationConfigApplicationContext sparkConf = new AnnotationConfigApplicationContext("sparkConf");
        //InfraService infraService = sparkConf.getBean(InfraService.class);

        log.setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("PokerStreamingApp");
        sparkConf.setMaster("local[2]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // JavaSparkContext sc = infraService.initSparkContext();
        // SQLContext sqlContext = new SQLContext(sc);
        SQLContext sqlContext = new SQLContext(javaSparkContext);

        // JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(5));
        JavaStreamingContext ssc = new JavaStreamingContext(javaSparkContext, Durations.seconds(10));
        ssc.checkpoint(".");




//        SparkConf sparkConf = new SparkConf();
//        sparkConf.setAppName("wordCountApp");
//        sparkConf.setMaster("local[2]");

//        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
//        ssc.checkpoint(".");

//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//        SQLContext sqlContext = new SQLContext(sc);

        // Initial state RDD input to mapWithState
        @SuppressWarnings("unchecked")

        List<Tuple2<Integer, Session>> tuples =
                Arrays.asList(new Tuple2<>(1, new Session()),
                        new Tuple2<>(2, new Session()));
        /*
        List<Tuple2<String, CustomState>> tuples =
                Arrays.asList(new Tuple2<>("hello", new CustomState(1, 1)),
                        new Tuple2<>("world", new CustomState(1, 1)));
        */

        // JavaPairRDD<String, CustomState> initialRDD = ssc.sparkContext().parallelizePairs(tuples);

        JavaPairRDD<Integer, Session> initialRDD = ssc.sparkContext().parallelizePairs(tuples);

        /*JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
                "localhost", Integer.parseInt("9999"), StorageLevels.MEMORY_AND_DISK_SER_2);
                */

        JavaReceiverInputDStream<String> events = ssc.socketTextStream(
                "localhost", Integer.parseInt("9999"), StorageLevels.MEMORY_AND_DISK_SER_2);

        JavaPairDStream javaPairDStream = events.mapToPair(EventService::parseStringToTuple2);

        // JavaDStream<String> words = lines.flatMap(WordsUtil::getWords);

        //JavaPairDStream<String, Integer> wordsDstream = words.mapToPair(
          //      s -> new Tuple2<>(s, 1));


        Function3<Integer, Optional<Event>, State<Session>, Tuple2<Integer, Session>> mappingFunc =
                (playerSessionId, event, sessionState) -> {

                    Session session = new Session();

                    // key exists in state
                    if (sessionState.exists()){

                        System.out.println("key exists in state");

                        if (event.get().serverDateTime.isAfter(sessionState.get().lastServerDatetime))
                            session.lastServerDatetime = event.get().serverDateTime;

                        // Login Event
                        if (Objects.isNull(sessionState.get().loginEvent))
                        {
                            System.out.println("login event NOT exists in state");
                            session.loginEvent = event.get();
                        }

                        else
                            System.out.println("login event exists in state");
                    }

                    else {
                        System.out.println("key NOT exists in state");
                        session.loginEvent = event.get();
                        session.lastServerDatetime = event.get().serverDateTime;
                    }



                    sessionState.update(session);
                    System.out.println("**************************" +
                            "state was updated" +
                            "**************************");
                    System.out.println(sessionState);

                    return new Tuple2<>(playerSessionId, session);


                    // parse json to get all the attributes


                    // state mapping function should:
                    // 1. update state when needed.
                    // 2. check if the event is valid (for example, 2nd login is not a valid
                    // 2. output the parsed JSON to big query

                };

        /*


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
                */


//        // DStream made of get cumulative counts that get updated in every batch
//        JavaMapWithStateDStream<String, Integer, CustomState, Tuple2<String, CustomState>> stateDstream =
//                wordsDstream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD));

        // DStream made of get cumulative counts that get updated in every batch
        JavaMapWithStateDStream<Integer, Optional<Event>, Session, Tuple2<Integer, Session>> stateDstream =
                javaPairDStream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD));

        stateDstream.print();
        ssc.start();
        ssc.awaitTermination();
    }
}


