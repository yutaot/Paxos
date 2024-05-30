package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Command;
import dslabs.framework.Message;
import java.util.Set;
import lombok.Data;

@Data
public final class PaxosRequest implements Message {
    // Your code here...
    private final String id;
    private final int sequenceNum;
    private final Command command;
}
