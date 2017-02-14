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

        // JavaSparkContext sc = infraService.initSparkContext();
        // SQLContext sqlContext = new SQLContext(sc);
        SQLContext sqlContext = new SQLContext(javaSparkContext);

        // JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(5));
        JavaStreamingContext ssc = new JavaStreamingContext(javaSparkContext, Durations.seconds(10));
        ssc.checkpoint("/home/cloudera/IdeaProjects/spark-streaming-poker/checkpoints/");




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

                    ArrayList<String> eventsWithoutDuration = new ArrayList<String>();
                    eventsWithoutDuration.add("simplifiedView");



                    // key exists in state
                    if (sessionState.exists()){

                        System.out.println("********** key exists in state **********");
                        session = sessionState.getOption().get();

                        // Update lastServerDatetime
                        if (event.get().serverDateTime.isAfter(sessionState.getOption().get().lastServerDatetime))
                            session.lastServerDatetime = event.get().serverDateTime;

                        // Login Event
                        // state: without login
                        // event: login
                        if (event.get().action.equals("login")
                                && Objects.isNull(session.loginEvent))
                        {

                            session.loginEvent = event.get();
                            sessionState.update(session);
                        }





                        // Window Event
                        if (event.get().object.equals("window"))
                        {
                            boolean isWindowExistsInState=false;

                            if (!session.windowEvents.isEmpty())
                            {
                                for (int i=0; i<session.windowEvents.size(); i++)
                                {
                                    // state: window exists
                                    if (session.windowEvents.get(i).windowId.equals(event.get().windowId))
                                    {
                                        isWindowExistsInState=true;
                                        // state: window open
                                        // event: close
                                        if (session.windowEvents.get(i).action.equals("open")
                                                && (event.get().action.equals("close")))
                                        {
                                            // add close data & login data
                                            System.out.println("********** state: window open; event: close **********");

                                            session.windowEvents.get(i).serverToDateTime = event.get().serverDateTime;
                                            session.windowEvents.get(i).clientToDateTime = event.get().clientDateTime;
                                            session.windowEvents.get(i).closeReason = event.get().closeReason;
                                            session.windowEvents.get(i).clientVersion = event.get().clientVersion;
                                            session.windowEvents.get(i).screen = event.get().screen;

                                            // write to big query
                                            EventService.writeToDestination(session.windowEvents.get(i));

                                            // close widgets if exists
                                            // ...

                                            // update state - remove window
                                            session.windowEvents.remove(i);
                                            sessionState.update(session);
                                            break;
                                        }

                                        // state: window close
                                        // event: open
                                        // open came before close
                                        if (session.windowEvents.get(i).action.equals("close")
                                                && (event.get().action.equals("open"))
                                                && event.get().serverDateTime.isBefore(session.windowEvents.get(i).serverToDateTime))
                                        {
                                            System.out.println("**********\n state: window close; event: open; open came before close\n**********");

                                            session.windowEvents.get(i).serverDateTime = event.get().serverDateTime;
                                            session.windowEvents.get(i).clientDateTime = event.get().clientDateTime;
                                            session.windowEvents.get(i).clientVersion = session.loginEvent.clientVersion;
                                            session.windowEvents.get(i).screen = session.loginEvent.screen;

                                            // write to big query
                                            EventService.writeToDestination(session.windowEvents.get(i));

                                            // remove window
                                            session.windowEvents.remove(i);
                                            sessionState.update(session);
                                            break;
                                        }
                                    }
                                }
                            }



                            // state: no window
                            // event: open
                            if (!isWindowExistsInState
                                && event.get().action.equals("open"))
                            {
                                System.out.println("**********\n state: no window; event: open\n**********");
                                EventService.writeToDestination(event.get());

                                session.windowEvents.add(event.get());

                                sessionState.update(session);
                            }

                            // state: no window
                            // event: close
                            if (!isWindowExistsInState
                                    && event.get().action.equals("close"))
                            {
                                System.out.println("**********\n state: no window; event: close\n**********");
                                event.get().serverToDateTime=event.get().serverDateTime;
                                event.get().serverDateTime=null;
                                event.get().clientToDateTime=event.get().clientDateTime;
                                event.get().clientDateTime=null;
                                session.windowEvents.add(event.get());

                                sessionState.update(session);
                            }
                        }

                        // Logout event
                        // state: login exists
                        // event: logout action
                        if (event.get().action.equals("logout")
                                && !Objects.isNull(session.loginEvent))
                        {
                            session.loginEvent.serverToDateTime = event.get().serverDateTime;
                            session.loginEvent.clientToDateTime = event.get().clientDateTime;

                            // write to big query
                            EventService.writeToDestination(session.loginEvent);

                            // Update state
                            sessionState.update(session);



                            // close open widgets
                            // ...

                            // close open widows
                            // ...

                        }

                        if (eventsWithoutDuration.contains(event.get().object))
                        {
                            event.get().clientVersion = session.loginEvent.clientVersion;
                            event.get().screen = session.loginEvent.screen;

                            // write to big query
                            EventService.writeToDestination(event.get());

                            // update last event datetime to state
                            sessionState.update(session);
                        }
                    }

                    else {
                        System.out.println("********** key NOT exists in state **********");
                        session.loginEvent = event.get();
                        session.lastServerDatetime = event.get().serverDateTime;

                        // write to big query
                        EventService.writeToDestination(session.loginEvent);

                        // Update state
                        sessionState.update(session);
                    }

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


