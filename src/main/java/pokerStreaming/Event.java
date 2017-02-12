package pokerStreaming;

import lombok.Getter;
import lombok.Setter;
import org.joda.time.DateTime;

import java.io.Serializable;

@Getter
@Setter

//@AllArgsConstructor
//@NoArgsConstructor

public class Event implements Serializable {
    Integer playerSessionId;
    String object;
    String objectType;
    String action;
    Integer cid;
    Integer loginId;
    Integer keepAlivePeriodInSeconds;
    Integer windowId;
    Integer tableId;
    Integer tourId;
    Integer snapInstanceId;
    DateTime clientDateTime;
    DateTime clientToDateTime;
    DateTime serverDateTime;
    DateTime serverToDateTime;
    Integer closeReason;
    String gameFormat;
    Integer isSnap;
    Integer IsBlast;
    String screen;
    String clientVersion;
    String rawDataJSON;
}
