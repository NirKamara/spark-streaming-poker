package wordCount;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.sql.Timestamp;


@Getter
//@Setter
//@AllArgsConstructor
//@NoArgsConstructor
public class Widget implements Serializable {

    String widgetType;
    Timestamp serverStartDateTime;
    Timestamp serverEndDateTime;
    Timestamp clientStartDateTime;
    Timestamp clientEndDateTime;

}
