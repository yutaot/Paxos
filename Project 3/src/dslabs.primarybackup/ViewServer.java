package dslabs.primarybackup;

import dslabs.framework.Address;
import dslabs.framework.Node;
import static dslabs.primarybackup.PingCheckTimer.PING_CHECK_MILLIS;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.java.Log;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class ViewServer extends Node {
    static final int STARTUP_VIEWNUM = 0;
    private static final int INITIAL_VIEWNUM = 1;

    // Your code here...
    private int currViewNum;
    private Address currPrimary;
    private Address currBackup;

    private int pingCheckTimerNum = 0;
    // keep track of each server's last seen ping check timer
    private final Map<Address, Integer> serverToPingCheckTimerMap = new HashMap<>();
    private int primaryLastSeenViewNum = STARTUP_VIEWNUM;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public ViewServer(Address address) {
        super(address);
    }

    @Override
    public void init() {
        set(new PingCheckTimer(), PING_CHECK_MILLIS);
        // Your code here...
        // initialize view
        currViewNum = STARTUP_VIEWNUM;
        currPrimary = null;
        currBackup = null;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePing(Ping m, Address sender) {
        // Your code here...

        boolean senderInMap = serverToPingCheckTimerMap.containsKey(sender);
        serverToPingCheckTimerMap.put(sender, pingCheckTimerNum);
        if (sender.equals(currPrimary) && m.viewNum() == currViewNum) {
            primaryLastSeenViewNum = currViewNum;
            if (currBackup == null) {
                // get potential backups that are confirmed to be alive at this point
                Map<Address, Integer> backupCandidateMap = serverToPingCheckTimerMap
                        .entrySet().stream()
                        .filter(entry -> entry.getValue() >= pingCheckTimerNum - 1)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                List<Address> backupCandidate = backupCandidateMap.keySet().stream()
                          .filter(address -> !address.equals(currPrimary) && !address.equals(currBackup))
                          .toList();
                if (!backupCandidate.isEmpty()) {
                    currBackup = backupCandidate.get(0);
                    currViewNum += 1;
                }
            }
        }

        // check if ViewServer just started
        if (currPrimary == null && currViewNum == STARTUP_VIEWNUM) {
            currPrimary = sender;
            primaryLastSeenViewNum = m.viewNum();
            currViewNum = INITIAL_VIEWNUM;
        }
        else if (!senderInMap && !sender.equals(currPrimary)) {
            if (currBackup == null && primaryLastSeenViewNum == currViewNum) {
                currBackup = sender;
                currViewNum += 1;
            }
        }
        // Ping was within timeframe, server is alive
        else if (serverToPingCheckTimerMap.get(sender) == pingCheckTimerNum - 1
                || serverToPingCheckTimerMap.get(sender) == pingCheckTimerNum) {
            // change view to make server a backup server
            if (currBackup == null && !sender.equals(currPrimary)) {
                updateView(sender);
            }
        }
        // Consider the server is dead
        else {
            serverToPingCheckTimerMap.remove(sender);
            // change view to change currPrimary or currBackup
            if (sender.equals(currPrimary) || sender.equals(currBackup)) {
                updateView(sender);
            }
        }
        // send ViewReply response
        View view = new View(currViewNum, currPrimary, currBackup);
        send(new ViewReply(view), sender);
    }

    private void handleGetView(GetView m, Address sender) {
        // Your code here...
        View view = new View(currViewNum, currPrimary, currBackup);
        send(new ViewReply(view), sender);
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onPingCheckTimer(PingCheckTimer t) {
        // Your code here...
        set(t, PING_CHECK_MILLIS);
        pingCheckTimerNum += 1;

        HashMap<Address, Integer> checkDeadServers = new HashMap<>(serverToPingCheckTimerMap);
        for (Map.Entry<Address, Integer> entry : checkDeadServers.entrySet()) {
            Address sender = entry.getKey();
            Integer pingCheckTimer = entry.getValue();

            // consider dead
            if (pingCheckTimer < pingCheckTimerNum - 1) {
                serverToPingCheckTimerMap.remove(sender);
                if (sender.equals(currPrimary) || sender.equals(currBackup)) {
                    updateView(sender);
                }
            }
        }
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
    private void updateView(Address sender) {
        if (primaryLastSeenViewNum != currViewNum) {
            return;
        }

        // get potential backups that are confirmed to be alive at this point
        Map<Address, Integer> backupCandidateMap = serverToPingCheckTimerMap
                .entrySet().stream()
                .filter(entry -> entry.getValue() >= pingCheckTimerNum - 1)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        List<Address> backupCandidate = backupCandidateMap.keySet().stream()
            .filter(address -> !address.equals(currPrimary) && !address.equals(currBackup))
            .toList();

//        List<Address> backupCandidate = serverToPingCheckTimerMap
//                .keySet().stream()
//                .filter(address -> !address.equals(currPrimary) && !address.equals(currBackup))
//                .toList();
        // currPrimary is dead
        if (sender.equals(currPrimary)) {
            if (currBackup != null) {
                if (!serverToPingCheckTimerMap.containsKey(currBackup)) {
                    return;
                }
                int pingCheckTimerBackup = serverToPingCheckTimerMap.get(currBackup);
                if (pingCheckTimerBackup >= pingCheckTimerNum - 1) {
                    currPrimary = currBackup;

                    if (!backupCandidate.isEmpty()) {
                        currBackup = backupCandidate.get(0);
                    } else {
                        currBackup = null;
                    }
                } else {
                    currBackup = null;
                }
            } else {
                return;
            }
        // currBackup is dead
        } else if (sender.equals(currBackup)) {
            if (!backupCandidate.isEmpty()) {
                currBackup = backupCandidate.get(0);
            } else {
                currBackup = null;
            }
        // make the new server a backup server
        } else {
            currBackup = sender;
        }
        currViewNum += 1;
    }
}
