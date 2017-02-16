package pokerStreaming;

import lombok.Getter;
import pokerStreaming.Event;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;


@Getter
//@Setter
//@AllArgsConstructor
//@NoArgsConstructor

public class Window implements Serializable {
    Event windowEvent;
    HashMap<String, Event> widgetEvents = new HashMap<>();

}
