package dslabs.clientserver;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import java.util.UUID;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.clientserver.ClientTimer.CLIENT_RETRY_MILLIS;

/**
 * Simple client that sends requests to a single server and returns responses.
 *
 * See the documentation of {@link Client} and {@link Node} for important
 * implementation notes.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class SimpleClient extends Node implements Client {
    private final Address serverAddress;

    // Your code here...
    private AMOCommand currAMOCommand;
    private Result result;
    private int sequenceNum = 0;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public SimpleClient(Address address, Address serverAddress) {
        super(address);
        this.serverAddress = serverAddress;
    }

    @Override
    public synchronized void init() {
        // No initialization necessary
    }

    /* -------------------------------------------------------------------------
        Client Methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command command) {
        // Your code here...

        int sequenceNumber = sequenceNum;
        AMOCommand amoCommand = new AMOCommand(command, sequenceNumber, this.address());
        currAMOCommand = amoCommand;
        result = null;

        this.send(new Request(amoCommand), serverAddress);
        this.set(new ClientTimer(amoCommand), CLIENT_RETRY_MILLIS);
    }

    @Override
    public synchronized boolean hasResult() {
        // Your code here...
        return result != null;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        // Your code here...
        while (result == null) {
            wait();
        }
        return result;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handleReply(Reply m, Address sender) {
        // Your code here...
        if (currAMOCommand != null && currAMOCommand.sequenceNum() == m.amoResult().sequenceNum()) {
            currAMOCommand = null;
            result = m.amoResult().result();
            sequenceNum += 1;
            notify();
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // Your code here...
        if (currAMOCommand != null && currAMOCommand.sequenceNum() == t.amoCommand().sequenceNum()) {
            send(new Request(currAMOCommand), serverAddress);
            set(t, CLIENT_RETRY_MILLIS);
        }
    }
}
