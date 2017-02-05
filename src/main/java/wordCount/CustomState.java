package wordCount;

import lombok.*;

import java.io.Serializable;

/**
 * Created by cloudera on 1/14/17.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class CustomState implements Serializable{
    int count;
    int count5;


}
