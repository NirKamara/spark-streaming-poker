package wordCount;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;


@Getter
//@Setter
//@AllArgsConstructor
//@NoArgsConstructor

public class Window implements Serializable {
    Event windowEvent;
    List<Event> widgetEvents;

}
