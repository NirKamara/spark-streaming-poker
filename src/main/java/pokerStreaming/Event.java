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
    String closeReason;
    String gameFormat;
    boolean isSnG;
    Integer isSnap;
    Integer isBlast;
    String screen;
    String clientVersion;
    String rawDataJSON1;
    String rawDataJSON2;
}
