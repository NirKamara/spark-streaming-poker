package toBeDeleted;

import lombok.*;
import pokerStreaming.Session;

import java.util.List;

/**
 * Created by cloudera on 1/14/17.
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PlayerSessionState {
    // Session
    // EventData (list)
    Session sessionData;
    List<EventData> eventsData;




}
