package wordCount;

import lombok.*;

import java.util.List;

/**
 * Created by cloudera on 1/14/17.
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PlayerSessionState {
    // SessionData
    // EventData (list)
    SessionData sessionData;
    List<EventData> eventsData;




}
