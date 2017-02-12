package wordCount;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;


@AllArgsConstructor
@NoArgsConstructor
@Data

public class Session implements Serializable{
    Event loginEvent = new Event();
    List<Event> windowEvents = new ArrayList<Event>();
    DateTime lastServerDatetime;
    Integer keepAlivePeriodInSeconds;

//    public Session processWindowCloseEvent (Integer windowId, Event event, Session session) {
//
//    }


}

