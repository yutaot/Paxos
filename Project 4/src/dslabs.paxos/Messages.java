package dslabs.paxos;

// Your code here...

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.paxos.PaxosServer.PaxosLogSlot;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.Data;

@Data
class P1aMessage implements Message {
    private final Ballot ballot;
}

@Data
class P1bMessage implements Message {
    private final Ballot ballot;
    private final HashMap<Integer, Pvalue> PvalueRecords;
//    private final Map<Integer, AMOCommand> decisions;

}

@Data
class P2aMessage implements Message {
    private final int slotNum;
    private final Ballot ballot;
    private final PaxosRequest paxosRequest;
}

@Data
class P2bMessage implements Message {
    private final int slotNum;
    private final Ballot ballot;
    private final PaxosRequest paxosRequest;

}

@Data
class Propose implements Message {
    private final int slotNum;
    private final AMOCommand amoCommand;
}

@Data
class HeartbeatMessage implements Message {
    private final int slotOut;
}

@Data
class HeartbeatReply implements Message {
    private final int slotOut;
}