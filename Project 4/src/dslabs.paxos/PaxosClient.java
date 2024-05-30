package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import java.util.HashSet;
import java.util.Set;
import javax.swing.AbstractListModel;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.paxos.ClientTimer.CLIENT_RETRY_MILLIS;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class PaxosClient extends Node implements Client {
    private final Address[] servers;

    // Your code here...
    private PaxosRequest paxosRequest = null;
    private PaxosReply paxosReply = null;
    private int sequenceNum = -1;


    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosClient(Address address, Address[] servers) {
        super(address);
        this.servers = servers;
    }

    @Override
    public synchronized void init() {
        // No need to initialize
    }

    /* -------------------------------------------------------------------------
        Public methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command operation) {
        // Your code here...
        this.sequenceNum += 1;
        AMOCommand amoCommand = new AMOCommand(operation, this.address(), this.sequenceNum);
        this.paxosRequest = new PaxosRequest(this.address().toString(), this.sequenceNum, amoCommand);

        //        System.out.println(address()+" sending command: "+amoCommand.command());
        broadcast(this.paxosRequest, servers);
        set(new ClientTimer(this.sequenceNum), CLIENT_RETRY_MILLIS);
    }

    @Override
    public synchronized boolean hasResult() {
        // Your code here...
        return this.paxosReply != null;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        // Your code here...
        while (this.paxosReply == null) {
            wait();
        }
        Result result = ((AMOResult) this.paxosReply.result()).result();
        this.paxosReply = null;
//        System.out.println("client got result: "+result);
        return result;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
        // Your code here...
//        System.out.println(sender+" -> "+address()+" PaxosReply: "+m);
        if (paxosRequest != null && m.sequenceNum() == this.paxosRequest.sequenceNum()) {
            this.paxosRequest = null;
            this.paxosReply = m;
            notify();
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // Your code here...
        if (paxosRequest != null && t.sequenceNum() == this.paxosRequest.sequenceNum()) {
//            System.out.println(address()+" sending message again due to timer");
            broadcast(this.paxosRequest, servers);
            set(t, CLIENT_RETRY_MILLIS);
        }
    }
}
