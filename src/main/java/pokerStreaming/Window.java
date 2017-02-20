package pokerStreaming;

import lombok.Getter;
import java.io.Serializable;
import java.util.HashMap;

//@Getter

class Window implements Serializable {
    Event windowEvent;
    HashMap<String, Event> widgetEvents = new HashMap<>();
}
