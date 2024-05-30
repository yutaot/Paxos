package dslabs.primarybackup;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.framework.Result;
import dslabs.primarybackup.PBServer.PBResult;
import java.util.HashMap;
import java.util.Queue;
import lombok.Data;

/* -------------------------------------------------------------------------
    ViewServer Messages
   -----------------------------------------------------------------------*/
@Data
class Ping implements Message {
    private final int viewNum;
}

@Data
class GetView implements Message {
}

@Data
class ViewReply implements Message {
    private final View view;
}

/* -------------------------------------------------------------------------
    Primary-Backup Messages
   -----------------------------------------------------------------------*/
@Data
class Request implements Message {
    // TODO: client request ...
    private final AMOCommand amoCommand;
}

@Data
class Reply implements Message {
    //TODO: server response ...
    private final AMOResult amoResult;
}

@Data
class BackupRequest implements Message {
    // TODO: primary send backup request to backup ...
    private final AMOCommand amoCommand;
}

@Data
class BackupReply implements Message {
    // TODO: backup send reply to primary ...
    private final AMOCommand amoCommand;
    private final PBResult result;
}

@Data
class SyncRequest implements Message {
    // TODO: primary sync up with backup ...
    private final AMOApplication app;
    private final HashMap lastSentResultMap;
}

@Data
class SyncReply implements Message {
    // TODO: backup send reply to primary ...
    private final PBResult result;
}

// TODO: add more messages here ...
