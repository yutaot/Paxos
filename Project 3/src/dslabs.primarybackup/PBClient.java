package dslabs.primarybackup;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import dslabs.primarybackup.PBServer.InvalidView;
import dslabs.primarybackup.PBServer.PBResult;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.java.Log;

import static dslabs.primarybackup.ViewServer.STARTUP_VIEWNUM;
import static dslabs.primarybackup.ClientTimer.CLIENT_RETRY_MILLIS;

@Log
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBClient extends Node implements Client {
    private final Address viewServer;

    //TODO: declare fields for your implementation ...
    private Address clientPrimary;
    private Address clientBackup;
    private int clientViewNum = STARTUP_VIEWNUM;
    private Result result;
    private AMOCommand currAMOCommand;
    private int sequenceNum = 0;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PBClient(Address address, Address viewServer) {
        super(address);
        this.viewServer = viewServer;
    }

    @Override
    public synchronized void init() {
        // TODO: initialize fields ...

        this.send(new GetView(), viewServer);

    }

    /* -------------------------------------------------------------------------
        Client Methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command command) {
        //TODO: send command to server ...
        AMOCommand amoCommand = new AMOCommand(command, this.sequenceNum, this.address());
        currAMOCommand = amoCommand;
        result = null;

        this.send(new Request(amoCommand), clientPrimary);
        this.set(new ClientTimer(amoCommand), CLIENT_RETRY_MILLIS);

    }

    @Override
    public synchronized boolean hasResult() {
        //TODO: check whether there is result ...
        return result != null;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        //TODO: wait to get result ...
        while (result == null) {
            wait();
        }
        return result;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handleReply(Reply m, Address sender) {
        //TODO: check desired reply arrive ...
        if (m.amoResult().result().getClass() == InvalidView.class) {
            // get view from viewserver
            this.send(new GetView(), viewServer);
        } else {
            if (currAMOCommand != null && currAMOCommand.sequenceNum() == m.amoResult().sequenceNum()) {
                currAMOCommand = null;
                result = m.amoResult().result();
                sequenceNum += 1;
                notify();
            }
        }
        // if invalidview, the onClientTimer will handle the re-sending of the request with new view
    }

    private synchronized void handleViewReply(ViewReply m, Address sender) {
        //TODO: perform action when timer reach timeout ...

        // update client view of servers, part of initialization as well
        clientPrimary = m.view().primary();
        clientBackup = m.view().backup();
        clientViewNum = m.view().viewNum();

    }

    // TODO: add utils here ...

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // TODO: handle client request timeout ...
        // primary server actually dies, or message got lost on the way or way back
        if (currAMOCommand != null && currAMOCommand.sequenceNum() == t.amoCommand().sequenceNum()) {
            this.send(new GetView(), viewServer);
            this.send(new Request(currAMOCommand), clientPrimary);
            this.set(t, CLIENT_RETRY_MILLIS);
        }
    }
}
