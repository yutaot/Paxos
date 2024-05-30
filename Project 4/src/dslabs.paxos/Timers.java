package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 130;

    // Your code here...
    private final int sequenceNum;
}

// Your code here...
@Data
final class HeartbeatCheckTimer implements Timer {
    static final int HEARTBEAT_CHECK_MILLIS = 150;
    private final Address leader;
}

@Data
final class WaitLeaderElectionTimer implements Timer {
    static final int WAIT_LEADER_ELECTION_MILLIS = 500;
}

//@Data
//final class LeaderElectionTimer implements Timer {
//    static final int LEADER_ELECTION_MILLIS = 300;
//    private final Ballot ballot;
//}

@Data
final class P1aTimer implements Timer {
    static final int P1A_MILLIS = 100;
    private final P1aMessage p1aMessage;
}

@Data
final class P2aTimer implements Timer {
    static final int P2A_MILLIS = 130;
    private final P2aMessage p2aMessage;
}

@Data
final class HeartbeatTimer implements Timer {
    static final int HEARTBEAT_MILLIS = 50;
}