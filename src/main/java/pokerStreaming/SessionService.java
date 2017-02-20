package pokerStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;


@Component
class SessionService {

    public static void initSpark() throws InterruptedException, IOException {

        // load attributes from configuration file
        Properties prop = new Properties();
        InputStream input = new FileInputStream("config.properties");
        prop.load(input);
        final Integer KEEP_ALIVE_PERIOD_IN_SECONDS = 20; // Integer.parseInt(prop.getProperty("keepAlivePeriodInSeconds"));

        // init spark
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("PokerStreamingApp");
        sparkConf.setMaster("local[2]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaStreamingContext ssc = new JavaStreamingContext(javaSparkContext, Durations.seconds(5));
        ssc.checkpoint("/home/cloudera/IdeaProjects/spark-streaming-poker/checkpoints/");

        // Initial state RDD input to mapWithState
        @SuppressWarnings("unchecked")

        JavaReceiverInputDStream<String> events = ssc.socketTextStream(
                "localhost", Integer.parseInt("9999"), StorageLevels.MEMORY_AND_DISK_SER_2);

        JavaPairDStream javaPairDStream = events.flatMapToPair(EventService::parseStringToTuple2);
        javaPairDStream.print();

        Function3<Integer, org.apache.spark.api.java.Optional<Event>, State<Session>, Tuple2<Integer, Session>> mappingFunc =
                (playerSessionId, event, sessionState) -> {

                    Session session = new Session();

                    if (!sessionState.isTimingOut()) {

                        ArrayList<String> eventsWithoutDuration = new ArrayList<String>();
                        eventsWithoutDuration.add("simplifiedView");
                        eventsWithoutDuration.add("disconnect");
                        eventsWithoutDuration.add("keepAlive");


                        // key exists in state (login already arrived)
                        if (sessionState.exists()) {

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
                            if (eventsWithoutDuration.indexOf(event.get().object) != -1)
                                SessionService.eventWithoutDuration(event.get(), sessionState);

                            // Events Without Duration
                            if (event.get().object.equals("preferred_seat"))
                                SessionService.preferredSeat(event.get(), sessionState);

                            // Logout Event
                            if (event.get().object.equals("logout"))
                                SessionService.closeLogout(event.get(), sessionState);

                        }
                        // State: key not in state
                        // Event: login
                        else if (event.get().object.equals("login")) {
                            SessionService.openLogin(event.get(), sessionState);
                        }

                        return new Tuple2<>(playerSessionId, session);
                    } else {
                        System.out.println("Timing out!!");
                        return new Tuple2<>(playerSessionId, session);
                    }
                };

        // DStream made of get cumulative counts that get updated in every batch
        JavaMapWithStateDStream<Integer, Event, Session, Tuple2<Integer, Session>> stateDstream =
                javaPairDStream.mapWithState(StateSpec
                        .function(mappingFunc)
                        .timeout(Durations.seconds(KEEP_ALIVE_PERIOD_IN_SECONDS)));

        stateDstream.print();
        ssc.start();
        ssc.awaitTermination();
    }

    static void openLogin(Event event, State<Session> sessionState) {

        Session session = new Session();

        session.loginEvent = event;
        session.lastServerDatetime = event.serverDateTime;

        // write to big query
        EventService.writeToDestination(session.loginEvent);

        sessionState.update(session);
    }

    static void closeLogout(Event event, State<Session> sessionState) {

        Session session = sessionState.get();

        session.lastServerDatetime = event.serverDateTime;

        session.loginEvent.serverToDateTime = event.serverDateTime;
        session.loginEvent.clientToDateTime = event.clientDateTime;
        session.loginEvent.closeReason = event.closeReason;
        session.loginEvent.rawDataJSON2 = event.rawDataJSON1;

        // close windows if exists (and write to big query)
        HashMap<Integer, Window> windowEvents;
        windowEvents = session.windowEvents;

        if (!Objects.isNull(windowEvents)
                && !windowEvents.isEmpty()) {

            event.objectType = "window";
            for (Map.Entry<Integer, Window> pair : windowEvents.entrySet()) {

                System.out.println("close window from logout event");
                System.out.println(pair.getKey());

                event.windowId = pair.getKey();
                closeWindow(event, sessionState);
            }
        }

        // write to big query
        EventService.writeToDestination(session.loginEvent);

        // update state - remove open windows
        System.out.println("session removed from state");
        sessionState.remove();

    }

    static void closeWidget(Event event, State<Session> sessionState) {

        Session session = sessionState.get();

        if (/*!session.windowEvents.isEmpty()
                && */!Objects.isNull(session.windowEvents.get(event.windowId))) {

            Event widgetInSession = session.windowEvents.get(event.windowId).widgetEvents.get(event.object);

            if (Objects.isNull(widgetInSession)) {

                // widget not exists for window in state
                event.serverToDateTime = event.serverDateTime;
                event.serverDateTime = null;
                event.clientToDateTime = event.clientDateTime;
                event.clientDateTime = null;

                // write to big query widget data
                EventService.writeToDestination(event);

                session.windowEvents.get(event.windowId).widgetEvents.put(event.object, event);
                sessionState.update(session);
            }
            // state: widget open, event: widget close
            else if (widgetInSession.action.equals("open")) {

                widgetInSession.serverToDateTime = event.serverDateTime;
                widgetInSession.clientToDateTime = event.clientDateTime;
                widgetInSession.closeReason = event.closeReason;

                widgetInSession.screen = session.loginEvent.screen;
                widgetInSession.screen1Width = session.loginEvent.screen1Width;
                widgetInSession.screen1Height = session.loginEvent.screen1Height;
                widgetInSession.cid = session.loginEvent.cid;
                widgetInSession.loginId = session.loginEvent.loginId;

                // write to big query
                EventService.writeToDestination(widgetInSession);

                // update state - remove widget
                session.windowEvents.get(event.windowId).widgetEvents.remove(event.object);
                sessionState.update(session);
            }
        }
    }

    static void openWidget(Event event, State<Session> sessionState) {

        Session session = sessionState.get();

        if (!session.windowEvents.isEmpty()
                && !Objects.isNull(session.windowEvents.get(event.windowId))) {

            // New Widget in Window
            Event widgetInSession = session.windowEvents.get(event.windowId).widgetEvents.get(event.object);

            if (Objects.isNull(widgetInSession)) {
                session.windowEvents.get(event.windowId).widgetEvents.put(event.object, event);

                // write to big query widget data
                EventService.writeToDestination(event);

                sessionState.update(session);

            }
            // State: close widget, Event: widget open
            else if (widgetInSession.action.equals("close")
                    && event.serverDateTime.isBefore(widgetInSession.serverToDateTime)) {

                widgetInSession.serverDateTime = event.serverDateTime;
                widgetInSession.clientDateTime = event.clientDateTime;

                widgetInSession.screen = session.loginEvent.screen;
                widgetInSession.screen1Width = session.loginEvent.screen1Width;
                widgetInSession.screen1Height = session.loginEvent.screen1Height;
                widgetInSession.cid = session.loginEvent.cid;
                widgetInSession.loginId = session.loginEvent.loginId;

                // write to big query widget data
                EventService.writeToDestination(widgetInSession);

                // update state - remove window
                session.windowEvents.get(event.windowId).widgetEvents.remove(event.object);

                sessionState.update(session);
            }
        }
    }

    static void closeWindow(Event event, State<Session> sessionState) {

        Session session = sessionState.get();

        if (/*!session.windowEvents.isEmpty()
                && */!Objects.isNull(session.windowEvents.get(event.windowId))) {

            Event windowInSession = session.windowEvents.get(event.windowId).windowEvent;

            if (windowInSession.action.equals("open")) {
                // add close data & login data
                System.out.println("********** state: window open; event: close **********");

                windowInSession.serverToDateTime = event.serverDateTime;
                windowInSession.clientToDateTime = event.clientDateTime;
                windowInSession.closeReason = event.closeReason;

                windowInSession.screen = session.loginEvent.screen;
                windowInSession.screen1Width = session.loginEvent.screen1Width;
                windowInSession.screen1Height = session.loginEvent.screen1Height;
                windowInSession.cid = session.loginEvent.cid;
                windowInSession.loginId = session.loginEvent.loginId;

                // close widgets if exists (and write to big query)
                HashMap<String, Event> widgetEvents = session.windowEvents.get(event.windowId).widgetEvents;
                if (!Objects.isNull(widgetEvents)
                        && !widgetEvents.isEmpty()) {

                    event.objectType = "widget";
                    for (Map.Entry<String, Event> pair : widgetEvents.entrySet()) {

                        event.object = pair.getKey();
                        closeWidget(event, sessionState);
                    }
                }

                // write to big query window data
                EventService.writeToDestination(windowInSession);

                // update state - remove window
                session.windowEvents.remove(event.windowId);

                sessionState.update(session);
            }
        }
        else {
            System.out.println("**********\n state: no window; event: close window\n**********");

            event.serverToDateTime = event.serverDateTime;
            event.serverDateTime = null;
            event.clientToDateTime = event.clientDateTime;
            event.clientDateTime = null;

            EventService.writeToDestination(event);

            Window window = new Window();
            window.windowEvent = event;
            session.windowEvents.put(event.windowId, window);

            sessionState.update(session);

        }
    }

    static void openWindow(Event event, State<Session> sessionState) {

        Session session = sessionState.get();

        if (!session.windowEvents.isEmpty()
                && !Objects.isNull(session.windowEvents.get(event.windowId))) {

            Event windowInSession = session.windowEvents.get(event.windowId).windowEvent;

            if (!Objects.isNull(windowInSession)) {
                if (windowInSession.action.equals("close")
                        && event.serverDateTime.isBefore(windowInSession.serverToDateTime)) {

                    System.out.println("**********\n state: window close; event: open; open came before close\n**********");

                    windowInSession.serverDateTime = event.serverDateTime;
                    windowInSession.clientDateTime = event.clientDateTime;

                    windowInSession.screen = session.loginEvent.screen;
                    windowInSession.screen1Width = session.loginEvent.screen1Width;
                    windowInSession.screen1Height = session.loginEvent.screen1Height;
                    windowInSession.cid = session.loginEvent.cid;
                    windowInSession.loginId = session.loginEvent.loginId;

                    // close widgets if exists (and write to big query)
                    HashMap<String, Event> widgetEvents = session.windowEvents.get(event.windowId).widgetEvents;
                    if (!Objects.isNull(widgetEvents)
                            && !widgetEvents.isEmpty()) {

                        for (Map.Entry<String, Event> pair : widgetEvents.entrySet()) {
                            closeWidget((Event) pair.getValue(), sessionState);
                        }
                    }

                    // write to big query window data
                    EventService.writeToDestination(windowInSession);

                    // update state - remove window
                    session.windowEvents.remove(event.windowId);

                    sessionState.update(session);
                }
            }
        }
        else {
            System.out.println("**********\n state: no window; event: open\n**********");
            EventService.writeToDestination(event);

            Window window = new Window();
            window.windowEvent = event;
            session.windowEvents.put(event.windowId, window);

            sessionState.update(session);
        }
    }

    static void preferredSeat (Event event, State<Session> sessionState) {

        System.out.println("got here");
        Session session = sessionState.get();
        session.lastServerDatetime = event.serverDateTime;

        event.object += "_" + event.preferredSeatTableNum;

        // write to big query
        EventService.writeToDestination(event);

        sessionState.update(session);
    }

    static void eventWithoutDuration (Event event, State<Session> sessionState) {

        Session session = sessionState.get();
        session.lastServerDatetime = event.serverDateTime;

        // write to big query
        EventService.writeToDestination(event);

        sessionState.update(session);
    }
}

