package pokerStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
// import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.json.JSONObject;
import scala.Array;
import scala.Double;
import scala.Tuple2;

import java.util.*;

//import java.time.format.DateTimeFormatter;


public class EventService {

    // setup spark conf
    @Autowired
    private SparkConf sc;
    private SQLContext sqlContext;

    // parse string to event object
    public static ArrayList<Tuple2<Integer, Event>> parseStringToTuple2(String s)

    /**********************************************
     need to add validations:
     winId must have value
     */

    {
        try {
            ArrayList<Tuple2<Integer, Event>> tupleEvents = new ArrayList<>();
            Event event = new Event();
            JSONObject obj = new JSONObject(s);


            DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").withZoneUTC();

            // User Attributes

            // playerSessionId
            if (!obj.getJSONObject("user").isNull("playerSessionId"))
                event.playerSessionId = obj.getJSONObject("user").getInt("playerSessionId");

            // loginId
            if (!obj.getJSONObject("user").isNull("loginId"))
                event.loginId = obj.getJSONObject("user").getInt("loginId");

            // cid
            if (!obj.getJSONObject("user").isNull("cid"))
                event.cid = obj.getJSONObject("user").getInt("cid");


            // Event Attributes

            // object
            event.object = (!obj.getJSONObject("event").isNull("object")) ?
                    obj.getJSONObject("event").getString("object") : "";

            // objectType
            event.objectType = (!obj.getJSONObject("event").isNull("objectType")) ?
                 obj.getJSONObject("event").getString("objectType") : "";

            // action
            if (!obj.getJSONObject("event").isNull("action")) {
                if (obj.getJSONObject("event").isNull("object")) {
                    event.object = obj.getJSONObject("event").getString("action");
                }
                event.action = obj.getJSONObject("event").getString("action");
            }
            else event.action = "";

            // windowId
            if (!obj.getJSONObject("event").isNull("winId"))
                event.windowId = obj.getJSONObject("event").getInt("winId");

            // tableId
            if (!obj.getJSONObject("event").isNull("tableId"))
                event.tableId = obj.getJSONObject("event").getInt("tableId");

            // tourId
            if (!obj.getJSONObject("event").isNull("tourId"))
                event.tourId = obj.getJSONObject("event").getInt("tourId");

            // snapInstanceId
            if (!obj.getJSONObject("event").isNull("snapInstanceId"))
                event.snapInstanceId = obj.getJSONObject("event").getInt("snapInstanceId");

            // isSnG
            if (!obj.getJSONObject("event").isNull("isSitAndGol"))
                event.isSnG = obj.getJSONObject("event").getString("isSitAndGol").equals("true");

            // gameFormat
            if (!obj.getJSONObject("event").isNull("tableId"))
                event.gameFormat = "Ring";

            else if (!obj.getJSONObject("event").isNull("tourId")) {
                if (event.isSnG)
                    event.gameFormat = "SnG";
                else
                    event.gameFormat = "MTT";
            }

            // isBlast
            if (!obj.getJSONObject("event").isNull("subTypeAttrMask")) {
                int tourSubMask = obj.getJSONObject("event").getInt("subTypeAttrMask");
                event.isBlast = (tourSubMask & 32) != 0 ? 1 : 0;
            }

            // isSnap
            if (!obj.getJSONObject("event").isNull("tableId")
                    && !obj.getJSONObject("event").isNull("ringAttrMask"))
            {
                int ringMask = obj.getJSONObject("event").getInt("ringAttrMask");
                event.isSnap = (ringMask & 2^8) != 0 ? 1 : 0;
            }
            else if (!obj.getJSONObject("event").isNull("tourId")
                    && !obj.getJSONObject("event").isNull("attrMask")) {
                int tourMask = obj.getJSONObject("event").getInt("attrMask");
                event.isSnap = (tourMask & 2^28) != 0 ? 1 : 0;
            }

            // serverDatetime
            if (!obj.getJSONObject("event").isNull("serverTime")) {
                event.serverDateTime = fmt.parseDateTime(obj.getJSONObject("event").getString("serverTime"));
            }

            // clientDateTime
            if (!obj.getJSONObject("event").isNull("clientTime")) {
                event.clientDateTime = fmt.parseDateTime(obj.getJSONObject("event").getString("clientTime"));
            }

            // closeReason
            if (!obj.getJSONObject("event").isNull("endReason"))
                event.closeReason = obj.getJSONObject("event").getString("endReason");

            // screen
            if (!obj.getJSONObject("event").isNull("screen")) {
                event.screen = obj.getJSONObject("event").getString("screen");
                String screen1 = obj.getJSONObject("event").getString("screen").split(";")[0];
                event.screen1Width = Integer.parseInt(screen1.substring(screen1.indexOf("w")+1, screen1.indexOf("_",3)));
                event.screen1Height = Integer.parseInt(screen1.substring(screen1.indexOf("h")+1));
            }

            // clientVersion
            if (!obj.getJSONObject("event").isNull("versionId"))
                event.clientVersion = obj.getJSONObject("event").getString("versionId");

            // keepAlivePeriod
            if (!obj.getJSONObject("event").isNull("keepAlivePeriod"))
                event.keepAlivePeriodInSeconds = obj.getJSONObject("event").getInt("keepAlivePeriod");

            // rawDataJSON1
            event.rawDataJSON1 = obj.toString();

            // preferred_seat
            if (!obj.getJSONObject("event").isNull("settings")) {

                JSONObject preferredSeat = obj.getJSONObject("event").getJSONObject("settings");

                for (String tableNum : preferredSeat.keySet()) {
                    Event preferredSeatEvent = new Event(event);
                    preferredSeatEvent.preferredSeatTableNum = tableNum;

                    tupleEvents.add(new Tuple2<>(event.playerSessionId, preferredSeatEvent));
                }
            }
            else tupleEvents.add(new Tuple2<>(event.playerSessionId, event));

            System.out.println("valid json");

            return tupleEvents;

        } catch (JSONException ex) {
            System.out.println("not a valid json");
            System.out.println(ex.getMessage());
            System.out.println("{ \"exception\" : \"not a valid json\" }");

            ArrayList<Tuple2<Integer, Event>> tupleException = new ArrayList<>();

            Event eventException = new Event();
            eventException.objectType = "exception";
            eventException.object = "exception";
            eventException.action = "exception";

            tupleException.add(new Tuple2<>(-1, eventException));

            return tupleException;
        }
    }




    // validate row

    // write to big query
    public static void writeToDestination(Event event)
    {
        // will be changed in the future to big query destination
        System.out.println("\nstart event data");

        System.out.println("  object: " + event.object);
        System.out.println("  objectType: " + event.objectType);
        System.out.println("  action: " + event.action);
        System.out.println("  serverDateTime: " + event.serverDateTime);
        System.out.println("  serverToDateTime: " + event.serverToDateTime);
        System.out.println("  screen: " + event.screen);
        System.out.println("  screen width: " + event.screen1Width);
        System.out.println("  screen height: " + event.screen1Height);
        System.out.println("  clientVersion: " + event.clientVersion);
        System.out.println("  cid: " + event.cid);
        System.out.println("  windowId: " + event.windowId);
        System.out.println("  gameFormat: " + event.gameFormat);
        System.out.println("  isSnap: " + event.isSnap);
        System.out.println("  isSnG: " + event.isSnG);
        System.out.println("  isBlast: " + event.isBlast);
        System.out.println("  preferredSeatTableNum: " + event.preferredSeatTableNum);
        System.out.println("  rawJson: " + event.rawDataJSON1);

        System.out.println("end event data");
    }

    // insert into state

    // remove from state



}
