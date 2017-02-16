package pokerStreaming;

import java.util.*;


import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
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


public class Main {

    public static void main(String[] args) throws InterruptedException {
        //AnnotationConfigApplicationContext sparkConf = new AnnotationConfigApplicationContext("sparkConf");
        //InfraService infraService = sparkConf.getBean(InfraService.class);
        org.apache.log4j.BasicConfigurator.configure();


        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("PokerStreamingApp");
        sparkConf.setMaster("local[2]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(javaSparkContext);

        JavaStreamingContext ssc = new JavaStreamingContext(javaSparkContext, Durations.seconds(10));
        ssc.checkpoint("/home/cloudera/IdeaProjects/spark-streaming-poker/checkpoints/");




        // Initial state RDD input to mapWithState
        @SuppressWarnings("unchecked")

        List<Tuple2<Integer, Session>> tuples =
                Arrays.asList(new Tuple2<>(1, new Session()),
                        new Tuple2<>(2, new Session()));

        JavaPairRDD<Integer, Session> initialRDD = ssc.sparkContext().parallelizePairs(tuples);

        JavaReceiverInputDStream<String> events = ssc.socketTextStream(
                "localhost", Integer.parseInt("9999"), StorageLevels.MEMORY_AND_DISK_SER_2);

        JavaPairDStream javaPairDStream = events.flatMapToPair(EventService::parseStringToTuple2);
        javaPairDStream.print();

        Function3<Integer, Optional<Event>, State<Session>, Tuple2<Integer, Session>> mappingFunc =
                (playerSessionId, event, sessionState) -> {

                    Session session = new Session();

                    ArrayList<String> eventsWithoutDuration = new ArrayList<String>();
                    eventsWithoutDuration.add("simplifiedView");
                    eventsWithoutDuration.add("disconnect");
                    eventsWithoutDuration.add("keepAlive");



                    // key exists in state (login already arrived)
                    if (sessionState.exists()){

                        session = sessionState.get();

                        // Update lastServerDatetime
                        if (event.get().serverDateTime.isAfter(sessionState.getOption().get().lastServerDatetime))
                            session.lastServerDatetime = event.get().serverDateTime;

                        // Window Open Event
                        if (event.get().object.equals("window")
                                && event.get().action.equals("open"))
                            SessionService.openWindow(event.get(), sessionState);

                        // Window Close Event
                        if (event.get().object.equals("window")
                                && event.get().action.equals("close"))
                        SessionService.closeWindow(event.get(), sessionState);

                        // Widget Open Event
                        if (event.get().objectType.equals("widget")
                                && event.get().action.equals("open"))
                            SessionService.openWidget(event.get(), sessionState);

                        // Widget Close Event
                        if (event.get().objectType.equals("widget")
                                && event.get().action.equals("close"))
                            SessionService.closeWidget(event.get(), sessionState);

                        // Preferred Seat Event
                        if (eventsWithoutDuration.indexOf(event.get().object)!=-1)
                            SessionService.eventWithoutDuration(event.get(), sessionState);

                        // Events Without Duration
                        if (event.get().object.equals("preferred_seat"))
                            SessionService.preferredSeat(event.get(), sessionState);

                    }
                    // State: key not in state
                    // Event: login
                    else if (event.get().object.equals("login")) {
                        SessionService.openLogin(event.get(), sessionState);
                    }

                    return new Tuple2<>(playerSessionId, session);
                };

        // DStream made of get cumulative counts that get updated in every batch
        JavaMapWithStateDStream<Integer, Optional<Event>, Session, Tuple2<Integer, Session>> stateDstream =
                javaPairDStream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD));

        stateDstream.print();
        ssc.start();
        ssc.awaitTermination();
    }
}


