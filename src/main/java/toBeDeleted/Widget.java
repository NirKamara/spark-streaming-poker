package toBeDeleted;

import lombok.Getter;

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
