package wordCount;

import jdk.nashorn.internal.parser.JSONParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.JavaPairRDD;
// import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.json.JSONObject;
import scala.Tuple2;

import java.sql.Timestamp;
//import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Optional;


public class EventService {

    // setup spark conf
    @Autowired
    private SparkConf sc;
    private SQLContext sqlContext;

    // parse string to event object
    public static Tuple2 parseStringToTuple2 (String s)
    {
        try {
            JSONObject obj = new JSONObject(s);
            Event event = new Event();

            DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").withZoneUTC();

            // user attributes
            if (!obj.getJSONObject("user").isNull("playerSessionId"))
                    event.playerSessionId = obj.getJSONObject("user").getInt("playerSessionId");

            if (!obj.getJSONObject("user").isNull("loginId"))
                    event.loginId = obj.getJSONObject("user").getInt("loginId");

            if (!obj.getJSONObject("user").isNull("cid"))
                event.cid = obj.getJSONObject("user").getInt("cid");

            if (!obj.getJSONObject("user").isNull("keepAlivePeriod"))
                event.keepAlivePeriodInSeconds = obj.getJSONObject("user").getInt("keepAlivePeriod");

            // event attributes

            // object
            if (!obj.getJSONObject("event").isNull("object"))
                event.object= obj.getJSONObject("event").getString("object");

            // action
            if (!obj.getJSONObject("event").isNull("action")) {
                if (obj.getJSONObject("event").getString("action") == "login"){
                    event.object = "login";
                }

                event.action = obj.getJSONObject("event").getString("action");
            }

            // windowId
            if (!obj.getJSONObject("event").isNull("winId"))
                event.windowId = obj.getJSONObject("event").getInt("winId");

            // serverDatetime
            if (!obj.getJSONObject("event").isNull("serverTime")) {
                event.serverDateTime = fmt.parseDateTime(obj.getJSONObject("event").getString("serverTime"));
            }



            System.out.println("valid json");
            return new Tuple2<>(event.playerSessionId, event);

        } catch (JSONException ex) {
            System.out.println("not a valid json");
            return new Tuple2<>(-1, "{ \"exception\" : \"not a valid json\" }");
        }
    }

    // extract playerSessionId
//    public static Tuple2 eventStringToTuple2 (String s)
//    {
//        try {
//            JSONObject obj = new JSONObject(s);
//            Integer playerSessionId = Integer.parseInt(obj.getJSONObject("user").getString("playerSessionId"));
//            return new Tuple2<>(playerSessionId, obj);
//        }
//        catch (JSONException ex) {
//            return new Tuple2<>(-1, "{ \"exception\" : \"exception\" }");
//        }
//    }


    // parse row
    public void parseEventRawData (JavaReceiverInputDStream<String> eventRawData)
    {
        JavaPairDStream javaPairDStream = eventRawData.mapToPair(EventService::parseStringToTuple2);
    }



    // State update function (Update the cumulative count function)

    Function3<Integer, String, State<Session>, Tuple2<Integer, Session>> mappingFunc =
            (playerSessionId, event, sessionState) -> {

                Session session = new Session();
                sessionState.update(session);

                return new Tuple2<>(playerSessionId, session);


                // parse json to get all the attributes


                // state mapping function should:
                // 1. update state when needed.
                // 2. output the parsed JSON as

            };

    // validate row

    // write to bigquery

    // insert into state

    // remove from state



}
