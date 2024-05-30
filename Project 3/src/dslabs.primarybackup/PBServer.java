package dslabs.primarybackup;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;
import dslabs.framework.Result;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.java.Log;

import static dslabs.primarybackup.BackupRequestTimer.BACKUP_REQUEST_RETRY_MILLIS;
import static dslabs.primarybackup.PingTimer.PING_MILLIS;
import static dslabs.primarybackup.SyncRequestTimer.SYNC_REQUEST_RETRY_MILLIS;

@Log
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBServer extends Node {
    private final Address viewServer;

    public interface PBResult extends Result {
    }

    @Data
    public static final class InvalidView implements PBResult {
        @NonNull private final View view;
    }

    @Data
    public static final class BackupSuccess implements PBResult {
    }

    @Data
    public static final class SyncSuccess implements PBResult {
    }

    // TODO: declare fields for your implementation ...
    private AMOApplication app;
    private HashMap<Address, AMOResult> lastSentResultMap = new HashMap<>();
    private Address primary;
    private Address backup;
    private int viewNum;
    private Queue<AMOCommand> backupOrderInPrimaryQueue = new LinkedList<>();
    private boolean syncComplete = false;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    PBServer(Address address, Address viewServer, Application app) {
        super(address);
        this.viewServer = viewServer;

        // TODO: wrap app inside AMOApplication ...
        this.app = new AMOApplication<>(app);
    }

    @Override
    public void init() {
        // TODO: initialize fields ...

        this.send(new Ping(this.viewNum), viewServer);
        this.set(new PingTimer(), PING_MILLIS);

    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleRequest(Request m, Address sender) {
        // TODO: handle client request ...
        // primary server case
        if (this.address().equals(primary)) {
            if (backup != null){
                backupOrderInPrimaryQueue.add(m.amoCommand());
                if (syncComplete) {
                    send(new BackupRequest(backupOrderInPrimaryQueue.peek()), backup);
                    set(new BackupRequestTimer(backupOrderInPrimaryQueue.peek()), BACKUP_REQUEST_RETRY_MILLIS);
                }
            } else {
                if (app.alreadyExecuted(m.amoCommand())) {
                    send(new Reply(lastSentResultMap.get(sender)), sender);
                } else {
                    AMOResult result = app.execute(m.amoCommand());
                    send(new Reply(result), sender);
                    lastSentResultMap.put(sender, result);
                }
            }
        }
        // backup server case
        else if (this.address().equals(backup)) {
            // send invalid view since server thinks it is not primary (it may be primary, maybe not)
            send(new Reply(new AMOResult(new InvalidView(new View(this.viewNum, this.primary, this.backup)), 0)), sender);
        }
        // idle server case
        else {
            // send invalid view since server thinks it is not primary (it may be primary, maybe not)
            send(new Reply(new AMOResult(new InvalidView(new View(this.viewNum, this.primary, this.backup)), 0)), sender);
        }
    }

    private void handleViewReply(ViewReply m, Address sender) {
        // TODO: handle view reply from view server ...

        Address oldBackup = this.backup;
        this.primary = m.view().primary();
        this.backup = m.view().backup();
        this.viewNum = m.view().viewNum();

        if (this.address().equals(this.primary)) {

//            // first primary promoted
//            if (oldPrimary == null) {
//                backupOrderInPrimaryQueue = new LinkedList<>();
//            }
//            // when server is promoted to primary
//            if (oldPrimary != null && !oldPrimary.equals(primary)) {
//                backupOrderInPrimaryQueue = new LinkedList<>();
//            }

            // if old backup also changed, we want to Sync the kvstore
            if (backup != null && !backup.equals(oldBackup)) {
                send(new SyncRequest(app, lastSentResultMap), backup);
                set(new SyncRequestTimer(app, lastSentResultMap), SYNC_REQUEST_RETRY_MILLIS);
                syncComplete = false;
            }
        }
    }

    // TODO: your message handlers ...

    private void handleBackupRequest(BackupRequest m, Address sender) {
        if (this.address().equals(backup) && sender.equals(primary)) {
            if (!app.alreadyExecuted(m.amoCommand())) {
                AMOResult result = app.execute(m.amoCommand());
                lastSentResultMap.put(m.amoCommand().clientAddress(), result);
            }
            this.send(new BackupReply(m.amoCommand(), new BackupSuccess()), sender);
        }
    }

    private void handleBackupReply(BackupReply m, Address sender) {
        if (m.result().getClass() == BackupSuccess.class) {
            if (!backupOrderInPrimaryQueue.isEmpty() && backupOrderInPrimaryQueue.peek().equals(m.amoCommand())) {
                backupOrderInPrimaryQueue.remove();
            }

            // send back reply to client
            if (app.alreadyExecuted(m.amoCommand())) {
                send(new Reply(lastSentResultMap.get(m.amoCommand().clientAddress())), m.amoCommand().clientAddress());
            } else {
                AMOResult result = app.execute(m.amoCommand());
                send(new Reply(result), m.amoCommand().clientAddress());
                lastSentResultMap.put(m.amoCommand().clientAddress(), result);
            }

            if (!backupOrderInPrimaryQueue.isEmpty()) {
                send(new BackupRequest(backupOrderInPrimaryQueue.peek()), backup);
                set(new BackupRequestTimer(backupOrderInPrimaryQueue.peek()), BACKUP_REQUEST_RETRY_MILLIS);
            }
        }
    }

    private void handleSyncRequest(SyncRequest m, Address sender) {
        if (!this.address().equals(this.primary)) {
            app = m.app();
            lastSentResultMap = m.lastSentResultMap();
            syncComplete = true;
            this.send(new SyncReply(new SyncSuccess()), sender);
        }
    }

    private void handleSyncReply(SyncReply m, Address sender) {
        if (m.result().getClass() == SyncSuccess.class) {
            syncComplete = true;

            if (!backupOrderInPrimaryQueue.isEmpty()) {
                send(new BackupRequest(backupOrderInPrimaryQueue.peek()), backup);
                set(new BackupRequestTimer(backupOrderInPrimaryQueue.peek()), BACKUP_REQUEST_RETRY_MILLIS);
            }
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onPingTimer(PingTimer t) {
        // TODO: on ping timeout ...
        this.send(new Ping(this.viewNum), viewServer);
        this.set(t, PING_MILLIS);
    }

    // TODO: your message handlers ...

    private void onBackupRequestTimer(BackupRequestTimer t) {
        // TODO: on backup request timeout ...
        if (t.amoCommand().equals(backupOrderInPrimaryQueue.peek())) {
            this.send(new BackupRequest(t.amoCommand()), this.backup);
            this.set(t, BACKUP_REQUEST_RETRY_MILLIS);
        }
    }

    private void onSyncRequestTimer(SyncRequestTimer t) {
        // TODO: on sync request timeout ...
        if (!syncComplete) {
            this.send(new SyncRequest(t.app(), t.lastSentResultMap()), backup);
            this.set(t, BACKUP_REQUEST_RETRY_MILLIS);
        }
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // TODO: add utils here ...
}
