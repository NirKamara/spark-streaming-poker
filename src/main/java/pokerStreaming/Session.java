package pokerStreaming;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.DateTime;

import java.io.InterruptedIOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


@AllArgsConstructor
@NoArgsConstructor
@Data

public class Session implements Serializable {
    Event loginEvent = new Event();
    HashMap<Integer, Window> windowEvents = new HashMap<>();
    DateTime lastServerDatetime;
    Integer keepAlivePeriodInSeconds;
}

//    public Session processWindowCloseEvent (Integer windowId, Event event, Session session) {
//
//    }





