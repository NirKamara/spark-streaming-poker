package pokerStreaming;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.joda.time.DateTime;

import java.io.Serializable;

@Getter
@Setter

//@AllArgsConstructor
//@NoArgsConstructor

public class Event implements Serializable, Cloneable {
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
    String preferredSeatTableNum;
    String screen;
    Integer screen1Width;
    Integer screen1Height;
    String clientVersion;
    String rawDataJSON1;
    String rawDataJSON2;

    public Event() {

    }

    public Event (Event originalEvent) {
        this.preferredSeatTableNum = originalEvent.preferredSeatTableNum;
        this.object = originalEvent.object;
        this.action= originalEvent.action;
        this.cid= originalEvent.cid;
        this.clientDateTime= originalEvent.clientDateTime;
        this.clientToDateTime = originalEvent.clientToDateTime;
        this.clientVersion = originalEvent.clientVersion;
        this.closeReason = originalEvent.closeReason;
        this.gameFormat = originalEvent.gameFormat;
        this.isBlast = originalEvent.isBlast;
        this.isSnap= originalEvent.isSnap;
        this.isSnG= originalEvent.isSnG;
        this.keepAlivePeriodInSeconds= originalEvent.keepAlivePeriodInSeconds;
        this.loginId= originalEvent.loginId;
        this.objectType= originalEvent.objectType;
        this.playerSessionId= originalEvent.playerSessionId;
        this.rawDataJSON1= originalEvent.rawDataJSON1;
        this.rawDataJSON2 = originalEvent.rawDataJSON2;
        this.screen = originalEvent.screen;
        this.screen1Height= originalEvent.screen1Height;
        this.screen1Width= originalEvent.screen1Width;
        this.serverDateTime= originalEvent.serverDateTime;
        this.serverToDateTime= originalEvent.serverToDateTime;
        this.snapInstanceId= originalEvent.snapInstanceId;
        this.tableId= originalEvent.tableId;
        this.tourId= originalEvent.tourId;
        this.windowId= originalEvent.windowId;

    }

}
