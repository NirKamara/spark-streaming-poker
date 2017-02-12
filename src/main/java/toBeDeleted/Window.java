package toBeDeleted;

import lombok.Getter;
import pokerStreaming.Event;

import java.io.Serializable;
import java.util.List;


@Getter
//@Setter
//@AllArgsConstructor
//@NoArgsConstructor

public class Window implements Serializable {
    Event windowEvent;
    List<Event> widgetEvents;

}
