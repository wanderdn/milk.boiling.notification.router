package org.milk.boiling.notification.dto.websocket;

import org.milk.boiling.notification.dto.DTOInterface;

public enum MilkBoilingMessageType implements DTOInterface {

    SUBSCRIBE,
    UNSUBSCRIBE,
    PAYLOAD,

}
