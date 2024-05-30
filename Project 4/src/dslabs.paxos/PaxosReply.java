package dslabs.paxos;

import dslabs.atmostonce.AMOResult;
import dslabs.framework.Message;
import dslabs.framework.Result;
import lombok.Data;

@Data
public final class PaxosReply implements Message {
    // Your code here...
    private final String id;
    private final int sequenceNum;
    private final Result result;
}
