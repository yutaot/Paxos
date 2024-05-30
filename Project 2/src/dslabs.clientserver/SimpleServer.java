package dslabs.clientserver;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore;
import dslabs.kvstore.KVStore.KVStoreResult;
import java.util.HashMap;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Simple server that receives requests and returns responses.
 *
 * See the documentation of {@link Node} for important implementation notes.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class SimpleServer extends Node {
    // Your code here...
    private final AMOApplication<Application> app;
    private final HashMap<Address, AMOResult> lastSentResultMap = new HashMap<>();

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public SimpleServer(Address address, Application app) {
        super(address);

        // Your code here...
        this.app = new AMOApplication<>(app);
    }

    @Override
    public void init() {
        // No initialization necessary
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleRequest(Request m, Address sender) {
        // Your code here...
        if (app.alreadyExecuted(m.amoCommand())) {
            send(new Reply(lastSentResultMap.get(sender)), sender);
        } else {
            AMOResult result = app.execute(m.amoCommand());
            send(new Reply(result), sender);
            lastSentResultMap.put(sender, result);
        }
    }
}
