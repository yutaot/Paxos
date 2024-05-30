package dslabs.primarybackup;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Timer;
import java.util.HashMap;
import lombok.Data;

@Data
final class PingCheckTimer implements Timer {
    static final int PING_CHECK_MILLIS = 100;
}

@Data
final class PingTimer implements Timer {
    static final int PING_MILLIS = 25;
}

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 100;

    // Your code here...
    private final AMOCommand amoCommand;
}

// Your code here...
@Data
final class BackupRequestTimer implements Timer {
    // TODO: choose an appropriate timer length, set intuitive length for now
    static final int BACKUP_REQUEST_RETRY_MILLIS = 100;
    private final AMOCommand amoCommand;
}

@Data
final class SyncRequestTimer implements Timer {
    static final int SYNC_REQUEST_RETRY_MILLIS = 30;
    private final AMOApplication app;
    private final HashMap lastSentResultMap;

}