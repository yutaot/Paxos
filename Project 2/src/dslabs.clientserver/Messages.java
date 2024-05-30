package dslabs.clientserver;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Message;
import lombok.Data;

@Data
class Request implements Message {
    private final AMOCommand amoCommand;
}

@Data
class Reply implements Message {
    private final AMOResult amoResult;
}
