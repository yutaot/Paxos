package dslabs.atmostonce;

import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.UUID;
import lombok.Data;

@Data
public final class AMOResult implements Result {
    // Your code here...
    private final Result result;
    private final int sequenceNum;
}
