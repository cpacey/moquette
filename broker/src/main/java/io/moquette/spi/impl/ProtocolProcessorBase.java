package io.moquette.spi.impl;

import io.moquette.parser.proto.messages.*;
import io.netty.channel.Channel;

/**
 * Created by cpacey on 01/04/16.
 */
public interface ProtocolProcessorBase {
    void processConnect(Channel channel, ConnectMessage msg);

    void processPubAck(Channel channel, PubAckMessage msg);

    void processPublish(Channel channel, PublishMessage msg);

    void processPubRel(Channel channel, PubRelMessage msg);

    void processPubRec(Channel channel, PubRecMessage msg);

    void processPubComp(Channel channel, PubCompMessage msg);

    void processDisconnect(Channel channel) throws InterruptedException;

    void processConnectionLost(String clientID, boolean sessionStolen, Channel channel);

    void processUnsubscribe(Channel channel, UnsubscribeMessage msg);

    void processSubscribe(Channel channel, SubscribeMessage msg);

    void notifyChannelWritable(Channel channel);

    void internalPublish(PublishMessage msg);
}
