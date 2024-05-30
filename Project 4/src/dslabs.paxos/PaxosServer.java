package dslabs.paxos;

import com.google.common.base.Objects;
import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Node;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.zip.CheckedOutputStream;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.checkerframework.checker.units.qual.A;

import static dslabs.paxos.HeartbeatCheckTimer.HEARTBEAT_CHECK_MILLIS;
import static dslabs.paxos.HeartbeatTimer.HEARTBEAT_MILLIS;
import static dslabs.paxos.P1aTimer.P1A_MILLIS;
import static dslabs.paxos.P2aTimer.P2A_MILLIS;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
    static final int INITIAL_BALLOT_NUMBER = 0;
    private final Address[] servers;

    // TODO: declare fields for your implementation ...
    private final Address rootAddress;
    private final AMOApplication<Application> app;

    /* for garbage collection log update */
    private int nextClearSlot;

    /* for execution detection, whether the request executed before (< this.slot_out) */
    private final HashMap<String, Pair<HashMap<Integer, Integer>, Integer>> ExecutedRecords = new HashMap<>();

    /* for replica */
    @Data
    static final class PaxosLogSlot implements Serializable {
        private final PaxosLogSlotStatus status;
        private final PaxosRequest paxosRequest;
    }

    private final HashMap<Integer, PaxosLogSlot> PaxosLog = new HashMap<>();
    // Current Client Requests in Log: PaxosRequest -> slots
    private final HashMap<String, HashMap<PaxosRequest, HashSet<Integer>>> RequestSlotRecords = new HashMap<>();
    private int slotIn, slotOut;  // slotIn: Next To Fill; slotOut: Next To Decide

    /* for leader */
    private boolean active;
    private Ballot leaderBallot;

    /* for acceptors */
    private Ballot acceptorBallot;
    private final HashMap<Integer, Pvalue> PvalueRecords = new HashMap<>();

    // client request - in-order generation assumption
    private final HashMap<String, PaxosRequest> ClientRequest = new HashMap<>();
    private Pair waitForP1b = null;
    private HashMap<Integer, Pvalue> AcceptorPvalues = new HashMap<>();
    private HashMap<Integer, Set<Address>> proposeRequestsWaitForMajority = new HashMap<>();
    private HashMap<Integer, P2aMessage> proposeRequestMap = new HashMap<>();
    private int heartbeat = 0;
    private Pair lastSeenLeaderHeartbeat = null;
    Map<Address, Integer> garbageCollectionMap = new HashMap<>();
    private int minSlotOut = 1;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosServer(Address address, Address[] servers, Application app) {
        super(address);
        this.servers = servers;

        // TODO: wrap app inside AMOApplication ...
        this.rootAddress = null;
        this.app = new AMOApplication<>(app);
    }

    @Override
    public void init() {
        // TODO: initialize fields ...
        this.nextClearSlot = 1;
        this.slotIn = 1;
        this.slotOut = 1;

        this.active = false;
        this.leaderBallot = new Ballot(INITIAL_BALLOT_NUMBER + 1, this.address());

        this.acceptorBallot = new Ballot(INITIAL_BALLOT_NUMBER, this.address());
        this.issuePhase1Request();
    }


    /* -------------------------------------------------------------------------
        Interface Methods

        Be sure to implement the following methods correctly. The test code uses
        them to check correctness more efficiently.
       -----------------------------------------------------------------------*/

    /**
     * Return the status of a given slot in the servers's local log.
     *
     * Log slots are numbered starting with 1.
     *
     * @param logSlotNum
     *         the index of the log slot
     * @return the slot's status
     */
    public PaxosLogSlotStatus status(int logSlotNum) {
        // TODO: return slot status in logSlotNum ...
        if (this.PaxosLog.containsKey(logSlotNum)) {
            return this.PaxosLog.get(logSlotNum).status(); // nextClear - slotIn
        }
        if (logSlotNum < this.nextClearSlot)
            return PaxosLogSlotStatus.CLEARED;
        assert logSlotNum >= this.slotIn : String.format("Slot Status Not Expected! (CLEAR: %s, OUT: %s, IN: %s)", this.nextClearSlot, this.slotOut, this.slotIn);
        return PaxosLogSlotStatus.EMPTY;
    }

    /**
     * Return the command associated with a given slot in the server's local
     * log. If the slot has status {@link PaxosLogSlotStatus#CLEARED} or {@link
     * PaxosLogSlotStatus#EMPTY}, this method should return {@code null}. If
     * clients wrapped commands in {@link dslabs.atmostonce.AMOCommand}, this
     * method should unwrap them before returning.
     *
     * Log slots are numbered starting with 1.
     *
     * @param logSlotNum
     *         the index of the log slot
     * @return the slot's contents or {@code null}
     */
    public Command command(int logSlotNum) {
        // TODO: return command assigned in logSlotNum ...
        if (!this.PaxosLog.containsKey(logSlotNum))
            return null;
        PaxosLogSlot paxosLogSlot = this.PaxosLog.get(logSlotNum);
        assert paxosLogSlot.status != PaxosLogSlotStatus.CLEARED : "Slot Statue As Cleared!";
        if (paxosLogSlot.status() == PaxosLogSlotStatus.EMPTY)
            return null;
        Command command = paxosLogSlot.paxosRequest().command();
        assert command instanceof AMOCommand : "Command Not As AMOCommand";
        return ((AMOCommand) command).command();
    }

    /**
     * Return the index of the first non-cleared slot in the server's local
     * log.
     *
     * Log slots are numbered starting with 1.
     *
     * @return the index in the log
     */
    public int firstNonCleared() {
        // TODO: return first slot that is not garbage collected yet ...
        return this.nextClearSlot;
    }

    /**
     * Return the index of the last non-empty slot in the server's local log. If
     * there are no non-empty slots in the log, this method should return 0.
     *
     * Log slots are numbered starting with 1.
     *
     * @return the index in the log
     */
    public int lastNonEmpty() {
        // TODO: return last non empty slot in paxos log ...
        return this.slotIn - 1;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePaxosRequest(PaxosRequest m, Address sender) {
        // TODO: handle paxos request ...

//        System.out.println("got request: "+m);

        // handle readonly request
        if (m.command().readOnly()) {
            if (this.app != null) {
                send(new PaxosReply(m.id(), m.sequenceNum(), this.app.executeReadOnly(m.command())), sender);
            }
            return;
        }

        // send back decisions - slot_out check
        if (this.whetherRequestComplete(m)) {
            if (this.app!= null) {
                this.sendRequestReply(m);
            }
            return;
        }

        // check whether in log already
        if (this.whetherRequestInProposal(m)) {
            return;
        }

        String id = m.id();
        // ignore the outdated client requests (in-order request generation assumption)
        if (!this.ClientRequest.containsKey(id) || this.ClientRequest.get(id).sequenceNum() < m.sequenceNum()) {
            this.ClientRequest.put(id, m);
            if (this.active)
                this.issuePhase2Request();
        }
    }

    // TODO: your message handlers ...

    private void handleP1aMessage(P1aMessage m, Address sender) {
        if (acceptorBallot.compareTo(m.ballot()) < 0) {
            acceptorBallot = m.ballot();
        }
        broadcast(new P1bMessage(acceptorBallot, new HashMap<>(PvalueRecords)), servers);
    }

    private void handleP1bMessage(P1bMessage m, Address sender) {
        if (m.ballot().compareTo(leaderBallot) > 0) {
            active = false;
            return;
        }
        if (waitForP1b != null && waitForP1b.getLeft().equals(m.ballot())) {
            Set<Address> waitFor = (Set<Address>) waitForP1b.getRight();
            waitFor.add(sender);
            AcceptorPvalues.putAll(m.PvalueRecords());
            if (waitFor.size() > servers.length/2) {
                waitForP1b = null;
                becomeLeader();
            } else {
                waitForP1b = new MutablePair(m.ballot(), waitFor);
            }
        }
    }

    private void handleP2aMessage(P2aMessage m, Address sender) {
        // check to send heartbeat or heartbeatcheck
        Address prevLeader = null;
        Address currLeader = sender;
        if (lastSeenLeaderHeartbeat != null)
            prevLeader = (Address) lastSeenLeaderHeartbeat.getRight();

        if (!active) {
            if (lastSeenLeaderHeartbeat == null || (prevLeader != null && !prevLeader.equals(currLeader))) {
                heartbeat = 0;
                lastSeenLeaderHeartbeat = new MutablePair(heartbeat, sender);
                set (new HeartbeatCheckTimer(sender), HEARTBEAT_CHECK_MILLIS);
            }
        } else if (active && (prevLeader == null || !prevLeader.equals(currLeader))) {
            lastSeenLeaderHeartbeat = new MutablePair(0, this.address());
            broadcast(new HeartbeatMessage(slotOut), servers);
            set(new HeartbeatTimer(), HEARTBEAT_MILLIS);
        }

//        System.out.println(acceptorBallot);
//        System.out.println(m.ballot());
        if (acceptorBallot.sequenceNum() == m.ballot().sequenceNum()) {
            PvalueRecords.put(m.slotNum(), new Pvalue(m.slotNum(), m.ballot(), m.paxosRequest()));
            updatePaxosLog(m.slotNum(), m.paxosRequest(), PaxosLogSlotStatus.ACCEPTED);
        }
//        System.out.println("sending back p2b message: "+m.paxosRequest());
        send(new P2bMessage(m.slotNum(), acceptorBallot, m.paxosRequest()), sender);
    }

    private void handleP2bMessage(P2bMessage m, Address sender) {
//        if (!m.ballot().equals(leaderBallot)) {
//            return;
//        }
        if (proposeRequestsWaitForMajority.containsKey(m.slotNum())) {
            Set<Address> waitFor = proposeRequestsWaitForMajority.get(m.slotNum());
            waitFor.add(sender);
            if (waitFor.size() > servers.length / 2) {
//                System.out.println(address()+" leader got majority!");
                proposeRequestsWaitForMajority.remove(m.slotNum());
                proposeRequestMap.remove(m.slotNum());
                sendDecision(m.slotNum(), m.paxosRequest());
            } else {
                proposeRequestsWaitForMajority.put(m.slotNum(), waitFor);
            }
        }
    }

    private void handleHeartbeatMessage(HeartbeatMessage m, Address sender) {
        if (!active) {
            lastSeenLeaderHeartbeat = new MutablePair(heartbeat, sender);
            //update logslots
            for (int i = 1; i < m.slotOut(); i++) {
                garbageCollectForHeartbeat(m.slotOut());
            }
            send(new HeartbeatReply(slotOut), sender);
        }
    }

    private void handleHeartbeatReply(HeartbeatReply m, Address sender) {
        if (active) {
            garbageCollectionMap.put(sender, m.slotOut());
            if (garbageCollectionMap.size() >= servers.length-1) {
                minSlotOut = Collections.min(garbageCollectionMap.values());
                for (int i = 1; i < minSlotOut; i++) {
                    if (PaxosLog.containsKey(i)) {
                        garbageCollectForHeartbeat(minSlotOut);
                    }
                }
                garbageCollectionMap.clear();
            }
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // TODO: your time handlers ...
    private void onP1aTimer(P1aTimer t) {
        if (waitForP1b != null && waitForP1b.getLeft().equals(t.p1aMessage().ballot())){
            leaderBallot = new Ballot(t.p1aMessage().ballot().sequenceNum()+servers.length, t.p1aMessage().ballot().address());;
            waitForP1b = new MutablePair(leaderBallot, new HashSet<Address>());
            P1aMessage p1aMessage = new P1aMessage(leaderBallot);
            broadcast(p1aMessage, servers);

            // TODO: maybe not this
            // clear this because we are trying leader election for new ballot again.
            AcceptorPvalues.clear();
            // TODO: maybe need to make timer more random
            set(new P1aTimer(p1aMessage), P1A_MILLIS*servers.length);
        }
    }

    private void onHeartbeatTimer(HeartbeatTimer t) {
        if (active) {
            broadcast(new HeartbeatMessage(slotOut), servers);
            set(new HeartbeatTimer(), HEARTBEAT_MILLIS);
        }
    }

    private void onHeartbeatCheckTimer(HeartbeatCheckTimer t) {
        // check if heartbeat was within the interval (and that this server is not leader)
        if (!active) {
            if (lastSeenLeaderHeartbeat == null)
                return;
            if ((Integer) lastSeenLeaderHeartbeat.getLeft() == heartbeat || (Integer) lastSeenLeaderHeartbeat.getLeft() == (heartbeat - 1)) {
                // update the interval
                heartbeat++;
                set(new HeartbeatCheckTimer(t.leader()), HEARTBEAT_CHECK_MILLIS);
            } else {
//                System.out.println(address()+" says i think leader is dead :(");
                lastSeenLeaderHeartbeat = null;
                issuePhase1Request();
            }
        }
    }

    private void onP2aTimer(P2aTimer t) {
        if (proposeRequestsWaitForMajority != null && proposeRequestsWaitForMajority.containsKey(t.p2aMessage().slotNum()) && proposeRequestMap.containsKey(t.p2aMessage().slotNum())
        && proposeRequestMap.get(t.p2aMessage().slotNum()).equals(t.p2aMessage())) {
            broadcast(t.p2aMessage(), servers);
            set(new P2aTimer(t.p2aMessage()), P2A_MILLIS);
        }
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // TODO: add utils here ...
    private boolean whetherRequestComplete(PaxosRequest paxosRequest) {
//        Address id = paxosRequest.id();
//        int sequenceNum = paxosRequest.sequenceNum();
//        Command command = paxosRequest.command();
//
//        AMOCommand amoCommand = new AMOCommand(command, id, sequenceNum);
//
//        if (app != null) {
//            return app.alreadyExecuted(amoCommand);
//        }
//        return false;
        String id = paxosRequest.id();
        return (this.ExecutedRecords.containsKey(id) && this.ExecutedRecords.get(id).getRight() >= paxosRequest.sequenceNum());
    }

    private boolean whetherRequestInProposal(PaxosRequest paxosRequest) {
        String id = paxosRequest.id();
        return (this.RequestSlotRecords.containsKey(id) && this.RequestSlotRecords.get(id).containsKey(paxosRequest));
    }

    private void updateExcutedRecord(int slotNum, PaxosRequest paxosRequest) {
        String id = paxosRequest.id().toString();
        int sequenceNum = paxosRequest.sequenceNum();
        if (!this.ExecutedRecords.containsKey(id))
            this.ExecutedRecords.put(id, new MutablePair<>(new HashMap<>(), -1));
        this.ExecutedRecords.get(id).getLeft().putIfAbsent(sequenceNum, slotNum);
        int currentMax = this.ExecutedRecords.get(id).getRight();
        this.ExecutedRecords.get(id).setValue(Math.max(sequenceNum, currentMax));
    }

    private void issuePhase1Request() {

        assert !this.active : "Repeat Issue Scout";

        P1aMessage p1aMessage = new P1aMessage(leaderBallot);
        broadcast(p1aMessage, servers);
        waitForP1b = new MutablePair<>(leaderBallot, new HashSet<Address>());

        set(new P1aTimer(p1aMessage), P1A_MILLIS);
    }

    private void becomeLeader() {
        // single server - the acceptor ballot must be smaller
        HashMap<Integer, Pvalue> pvalues = AcceptorPvalues;
        AcceptorPvalues.clear();

        for (int i : this.PvalueRecords.keySet()) {
            Pvalue pvalue = this.PvalueRecords.get(i);
            if (pvalues.containsKey(i) && pvalue.ballot().compareTo(pvalues.get(i).ballot()) < 0)
                continue;
            pvalues.put(i, pvalue);
        }

        this.active = true;
//        System.out.println(address()+" is leader");
        // send heartbeat message in intervals
//        broadcast(new HeartbeatMessage(leaderBallot, slotOut, slotIn, PaxosLog), servers);
//        set(new HeartbeatTimer(), HEARTBEAT_MILLIS);

        for (Map.Entry<Integer, Pvalue> entry : pvalues.entrySet())
            this.updatePaxosLog(entry.getKey(), entry.getValue().paxosRequest(), PaxosLogSlotStatus.ACCEPTED);
//        System.out.println(PaxosLog);
        this.issuePhase2Request();
    }

    private void issuePhase2Request() {
        assert this.active : "Non Active Leader Issue Phase 2";

        // get current request
        for (String id : this.ClientRequest.keySet()) {
            if (!(this.whetherRequestComplete(this.ClientRequest.get(id)) || this.whetherRequestInProposal(this.ClientRequest.get(id))))
                this.updatePaxosLog(this.slotIn, this.ClientRequest.get(id), PaxosLogSlotStatus.ACCEPTED);
        }
        this.ClientRequest.clear();

        // this is a easy version solution, there is no need to think much about empty slot
//        HashMap<Integer, PaxosRequest> proposeRequests = new HashMap<>();
        for (int i = this.slotOut; i < this.slotIn; i++) {
            PaxosLogSlot paxosLogSlot = this.PaxosLog.get(i);
            if (paxosLogSlot.status != PaxosLogSlotStatus.EMPTY) {
//                proposeRequests.put(i, paxosLogSlot.paxosRequest());
                P2aMessage p2aMessage = new P2aMessage(i, leaderBallot, paxosLogSlot.paxosRequest);
                broadcast(p2aMessage, servers);

//                System.out.println("broadcasting this message: "+p2aMessage);

                proposeRequestsWaitForMajority.put(i, new HashSet<>());
                proposeRequestMap.put(i, p2aMessage);

                set(new P2aTimer(p2aMessage), P2A_MILLIS);
            }
        }
    }

    private void sendDecision(int slotNum, PaxosRequest paxosRequest) {
        this.updatePaxosLog(slotNum, paxosRequest, PaxosLogSlotStatus.CHOSEN);
    }

    private void updateRequestRecord(int slotNum, PaxosRequest paxosRequest) {
        assert paxosRequest != null : "Slot Request As Null - Update Record";
        String id = paxosRequest.id();
        if (!this.RequestSlotRecords.containsKey(id))
            this.RequestSlotRecords.put(id, new HashMap<>());
        if (!this.RequestSlotRecords.get(id).containsKey(paxosRequest))
            this.RequestSlotRecords.get(id).put(paxosRequest, new HashSet<>());
        this.RequestSlotRecords.get(id).get(paxosRequest).add(slotNum);
    }

    private void removeRequestRecord(int slotNum, PaxosRequest paxosRequest) {
        assert paxosRequest != null : "Slot Request As Null - Remove Record";
        String id = paxosRequest.id();
        assert this.RequestSlotRecords.containsKey(id) : "Request Record Not Contain " + id;
        assert this.RequestSlotRecords.get(id).containsKey(paxosRequest) : "Request Record Not Contain Corresponding Paxos - " + id + "\n" + paxosRequest.toString();

        this.RequestSlotRecords.get(id).get(paxosRequest).remove(slotNum);
        if (this.RequestSlotRecords.get(id).get(paxosRequest).size() == 0)
            this.RequestSlotRecords.get(id).remove(paxosRequest);
        if (this.RequestSlotRecords.get(id).size() == 0)
            this.RequestSlotRecords.remove(id);
    }

    private void updatePaxosLog(int slotNum, PaxosRequest paxosRequest, PaxosLogSlotStatus status) {
        assert paxosRequest != null : "Update Paxos Log With Request As Null";
        if (slotNum < this.slotOut) {
            assert slotNum < this.nextClearSlot || status != PaxosLogSlotStatus.CHOSEN || Objects
                    .equal(this.PaxosLog.get(slotNum).paxosRequest(), paxosRequest) : "Try To Assign Different Paxos Log, Smaller Than Slot Out";
            return;
        }

        if (slotNum >= this.slotIn) {
            for (int i = this.slotIn; i < slotNum + 1; i++) {
                this.PaxosLog.put(i, new PaxosLogSlot(PaxosLogSlotStatus.EMPTY, null));
            }
            this.slotIn = slotNum + 1;
        }

        if (this.PaxosLog.get(slotNum).status == PaxosLogSlotStatus.CHOSEN) {
            assert Objects.equal(this.PaxosLog.get(slotNum).paxosRequest(), paxosRequest) : "Try To Assign Different Paxos Log, Already Chosen";
            return;
        }

        if (this.PaxosLog.get(slotNum).status == PaxosLogSlotStatus.ACCEPTED && !Objects.equal(this.PaxosLog.get(slotNum).paxosRequest(), paxosRequest))
            this.removeRequestRecord(slotNum, this.PaxosLog.get(slotNum).paxosRequest());

        this.PvalueRecords.put(slotNum, new Pvalue(slotNum, this.acceptorBallot, paxosRequest));
        this.PaxosLog.put(slotNum, new PaxosLogSlot(status, paxosRequest));
        this.updateRequestRecord(slotNum, paxosRequest);

        while (this.slotOut < this.slotIn) {
            if (this.PaxosLog.get(this.slotOut).status != PaxosLogSlotStatus.CHOSEN)
                break;
            PaxosRequest request = this.PaxosLog.get(this.slotOut).paxosRequest();
            this.sendRequestReply(request);
            this.updateExcutedRecord(this.slotOut, request);
            this.slotOut += 1;
        }

        // perform garbage collection
        for (int i = this.nextClearSlot; i < this.slotOut; i++) {
            assert this.PaxosLog.get(i).status() == PaxosLogSlotStatus.CHOSEN : "Slot Status As " + this.PaxosLog.get(i).status().toString() + ", Not Chosen - Garbage Clean";
            this.removeRequestRecord(i, this.PaxosLog.get(i).paxosRequest());
            this.PaxosLog.remove(i);
            this.PvalueRecords.remove(i);
        }
        this.nextClearSlot = this.slotOut;
    }

    private void garbageCollectForHeartbeat(int slotOut) {
//        for (int i = 1; i < slotOut; i++) {
////            if (this.PaxosLog.containsKey(i))
////                this.removeRequestRecord(i, this.PaxosLog.get(i).paxosRequest());
//            this.PaxosLog.remove(i);
//            this.PvalueRecords.remove(i);
//        }
//        if (slotOut > this.slotOut)
//            this.slotOut = slotOut;
//        this.nextClearSlot = this.slotOut;
    }

    private HashMap<AMOCommand, AMOResult> commandToResult = new HashMap<>();

    private void sendRequestReply(PaxosRequest paxosRequest) {
        if (this.app != null) {
            if (!(paxosRequest.command() instanceof AMOCommand))
                throw new IllegalArgumentException();
            AMOCommand amoCommand = (AMOCommand) paxosRequest.command();
            AMOResult amoResult = null;

            if (!this.app.alreadyExecuted(amoCommand)) {
                amoResult = this.app.execute(amoCommand);
                commandToResult.put(amoCommand, amoResult);
            }
//            System.out.println("sending back paxosreply for paxosrequest: " + paxosRequest);
            send(new PaxosReply(paxosRequest.id(), paxosRequest.sequenceNum(), commandToResult.get(amoCommand)), amoCommand.address());
        } else {
            assert this.rootAddress != null : "Sending Decision with Root Addrees AS NULL";
            this.handleMessage(new PaxosDecision(paxosRequest.id(), paxosRequest.sequenceNum(), paxosRequest.command()), this.rootAddress);
        }
    }
}