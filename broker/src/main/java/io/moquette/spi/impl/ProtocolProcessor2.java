/*
 * Copyright (c) 2012-2015 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package io.moquette.spi.impl;

import io.moquette.parser.proto.messages.*;
import io.moquette.parser.proto.messages.AbstractMessage.QOSType;
import io.moquette.server.ConnectionDescriptor;
import io.moquette.server.netty.AutoFlushHandler;
import io.moquette.server.netty.NettyUtils;
import io.moquette.spi.ClientSession;
import io.moquette.spi.IMessagesStore;
import io.moquette.spi.IMessagesStore.StoredMessage;
import io.moquette.spi.ISessionsStore;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.impl.subscriptions.SubscriptionsStore;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static io.moquette.parser.netty.Utils.VERSION_3_1;
import static io.moquette.parser.netty.Utils.VERSION_3_1_1;

/**
 * Class responsible to handle the logic of MQTT protocol it's the director of
 * the protocol execution. 
 *
 * Used by the front facing class SimpleMessaging.
 *
 * @author andrea
 */
public class ProtocolProcessor2 implements ProtocolProcessorBase {

    private static final Logger LOG = LoggerFactory.getLogger(ProtocolProcessor2.class);

    protected ConcurrentMap<String, ConnectionDescriptor> m_clientIDs;
    private SubscriptionsStore subscriptions;
    private ISessionsStore m_sessionsStore;

    ProtocolProcessor2() {}

    /**
     * @param subscriptions the subscription store where are stored all the existing
     *  clients subscriptions.
     * @param sessionsStore the clients sessions store, used to persist subscriptions.
     */
    void init(SubscriptionsStore subscriptions,
              ISessionsStore sessionsStore) {
        this.m_clientIDs = new ConcurrentHashMap<>();
        this.subscriptions = subscriptions;
        LOG.trace("subscription tree on init {}", subscriptions.dumpTree());
        m_sessionsStore = sessionsStore;
    }

    @Override
    public void processConnect(Channel channel, ConnectMessage msg) {
        LOG.debug("CONNECT for client <{}>", msg.getClientID());
        if (msg.getProtocolVersion() != VERSION_3_1 && msg.getProtocolVersion() != VERSION_3_1_1) {
            sendBadProtocolMessage(channel);
            return;
        }

        if (msg.getClientID() == null || msg.getClientID().length() == 0) {
            sendBadClientIdMessage(channel, ConnAckMessage.IDENTIFIER_REJECTED);
            return;
        }

        ConnectionDescriptor connDescr = new ConnectionDescriptor(msg.getClientID(), channel, msg.isCleanSession());
        m_clientIDs.put(msg.getClientID(), connDescr);

        int keepAlive = msg.getKeepAlive();
        LOG.debug("Connect with keepAlive {} s",  keepAlive);
        NettyUtils.keepAlive(channel, keepAlive);
        NettyUtils.cleanSession(channel, msg.isCleanSession());
        NettyUtils.clientID(channel, msg.getClientID());
        LOG.debug("Connect create session <{}>", channel);

        setIdleTime(channel.pipeline(), Math.round(keepAlive * 1.5f));

        ConnAckMessage okResp = new ConnAckMessage();
        okResp.setReturnCode(ConnAckMessage.CONNECTION_ACCEPTED);
        okResp.setSessionPresent(false);
        channel.writeAndFlush(okResp);

        int flushIntervalMs = 500/*(keepAlive * 1000) / 2*/;
        setupAutoFlusher(channel.pipeline(), flushIntervalMs);
        LOG.info("CONNECT processed");
    }

    private void sendBadClientIdMessage(Channel channel, byte identifierRejected) {
        ConnAckMessage okResp = new ConnAckMessage();
        okResp.setReturnCode(identifierRejected);
        channel.writeAndFlush(okResp);
    }

    private void sendBadProtocolMessage(Channel channel) {
        ConnAckMessage badProto = new ConnAckMessage();
        badProto.setReturnCode(ConnAckMessage.UNNACEPTABLE_PROTOCOL_VERSION);
        LOG.warn("processConnect sent bad proto ConnAck");
        channel.writeAndFlush(badProto);
        channel.close();
    }

    private void setupAutoFlusher(ChannelPipeline pipeline, int flushIntervalMs) {
        AutoFlushHandler autoFlushHandler = new AutoFlushHandler(flushIntervalMs, TimeUnit.MILLISECONDS);
        try {
            pipeline.addAfter("idleEventHandler", "autoFlusher", autoFlushHandler);
        } catch (NoSuchElementException nseex) {
            //the idleEventHandler is not present on the pipeline
            pipeline.addFirst("autoFlusher", autoFlushHandler);
        }
    }

    private void setIdleTime(ChannelPipeline pipeline, int idleTime) {
        if (pipeline.names().contains("idleStateHandler")) {
            pipeline.remove("idleStateHandler");
        }
        pipeline.addFirst("idleStateHandler", new IdleStateHandler(0, 0, idleTime));
    }

    @Override
    public void processPubAck(Channel channel, PubAckMessage msg) {
        channel.close();
    }

    private void verifyToActivate(String clientID, ClientSession targetSession) {
        if (m_clientIDs.containsKey(clientID)) {
            targetSession.activate();
        }
    }

    private static StoredMessage asStoredMessage(PublishMessage msg) {
        StoredMessage stored = new StoredMessage(msg.getPayload().array(), msg.getQos(), msg.getTopicName());
        stored.setRetained(msg.isRetainFlag());
        stored.setMessageID(msg.getMessageID());
        return stored;
    }

    @Override
    public void processPublish(Channel channel, PublishMessage msg) {
        LOG.trace("PUB --PUBLISH--> SRV executePublish invoked with {}", msg);
        String clientID = NettyUtils.clientID(channel);
        final String topic = msg.getTopicName();

        final QOSType qos = msg.getQos();
        LOG.info("PUBLISH from clientID <{}> on topic <{}> with QoS {}", clientID, topic, qos);

        if ( qos != QOSType.MOST_ONE ) {
            channel.close();
            return;
        }

        StoredMessage toStoreMsg = asStoredMessage(msg);
        toStoreMsg.setClientID(clientID);
        route2Subscribers(toStoreMsg);
    }

    /**
     * Intended usage is only for embedded versions of the broker, where the hosting application want to use the
     * broker to send a publish message.
     * Inspired by {@link #processPublish} but with some changes to avoid security check, and the handshake phases
     * for Qos1 and Qos2.
     * It also doesn't notifyTopicPublished because using internally the owner should already know where
     * it's publishing.
     * */
    public void internalPublish(PublishMessage msg) {
        final AbstractMessage.QOSType qos = msg.getQos();
        final String topic = msg.getTopicName();
        LOG.info("embedded PUBLISH on topic <{}> with QoS {}", topic, qos);

        IMessagesStore.StoredMessage toStoreMsg = asStoredMessage(msg);
        toStoreMsg.setClientID("BROKER_SELF");
        toStoreMsg.setMessageID(1);
        route2Subscribers(toStoreMsg);
    }

    /**
     * Flood the subscribers with the message to notify. MessageID is optional and should only used for QoS 1 and 2
     * */
    void route2Subscribers(StoredMessage pubMsg) {
        final String topic = pubMsg.getTopic();
        final QOSType publishingQos = pubMsg.getQos();
        final ByteBuffer origMessage = pubMsg.getMessage();
        LOG.debug("route2Subscribers republishing to existing subscribers that matches the topic {}", topic);
        if (LOG.isTraceEnabled()) {
            LOG.trace("content <{}>", DebugUtils.payload2Str(origMessage));
            LOG.trace("subscription tree {}", subscriptions.dumpTree());
        }

        for (final Subscription sub : subscriptions.matches(topic)) {
            QOSType qos = publishingQos;
            if (qos.byteValue() > sub.getRequestedQos().byteValue()) {
                qos = sub.getRequestedQos();
            }
            ClientSession targetSession = m_sessionsStore.sessionForClient(sub.getClientId());
            verifyToActivate(sub.getClientId(), targetSession);

            LOG.debug("Broker republishing to client <{}> topic <{}> qos <{}>, active {}",
                    sub.getClientId(), sub.getTopicFilter(), qos, targetSession.isActive());
            ByteBuffer message = origMessage.duplicate();

            if (qos == QOSType.MOST_ONE && targetSession.isActive()) {
                //QoS 0
                directSend(targetSession, topic, qos, message, false, null);
            }
        }
    }

    protected void directSend(ClientSession clientsession, String topic, QOSType qos,
                              ByteBuffer message, boolean retained, Integer messageID) {
        String clientId = clientsession.clientID;
        LOG.debug("directSend invoked clientId <{}> on topic <{}> QoS {} retained {} messageID {}",
                clientId, topic, qos, retained, messageID);
        PublishMessage pubMessage = new PublishMessage();
        pubMessage.setRetainFlag(retained);
        pubMessage.setTopicName(topic);
        pubMessage.setQos(qos);
        pubMessage.setPayload(message);

        LOG.info("send publish message to <{}> on topic <{}>", clientId, topic);
        if (LOG.isDebugEnabled()) {
            LOG.debug("content <{}>", DebugUtils.payload2Str(message));
        }
        //set the PacketIdentifier only for QoS > 0
        if (pubMessage.getQos() != QOSType.MOST_ONE) {
            pubMessage.setMessageID(messageID);
        } else {
            if (messageID != null) {
                throw new RuntimeException("Internal bad error, trying to forwardPublish a QoS 0 message " +
                        "with PacketIdentifier: " + messageID);
            }
        }

        if (m_clientIDs == null) {
            throw new RuntimeException("Internal bad error, found m_clientIDs to null while it should be " +
                    "initialized, somewhere it's overwritten!!");
        }
        //LOG.trace("clientIDs are {}", m_clientIDs);
        if (m_clientIDs.get(clientId) == null) {
            //TODO while we were publishing to the target client, that client disconnected,
            // could happen is not an error HANDLE IT
            throw new RuntimeException(String.format("Can't find a ConnectionDescriptor for client <%s> in cache <%s>",
                    clientId, m_clientIDs));
        }
        Channel channel = m_clientIDs.get(clientId).channel;
        LOG.trace("Session for clientId {}", clientId);
        if (channel.isWritable()) {
            //if channel is writable don't enqueue
            channel.write(pubMessage);
        } else {
            //enqueue to the client session
            clientsession.enqueue(pubMessage);
        }
    }

    /**
     * Second phase of a publish QoS2 protocol, sent by publisher to the broker. Search the stored message and publish
     * to all interested subscribers.
     * */
    @Override
    public void processPubRel(Channel channel, PubRelMessage msg) {
        channel.close();
    }

    @Override
    public void processPubRec(Channel channel, PubRecMessage msg) {
        channel.close();
    }

    @Override
    public void processPubComp(Channel channel, PubCompMessage msg) {
        channel.close();
    }

    @Override
    public void processDisconnect(Channel channel) throws InterruptedException {
        channel.flush();
        String clientID = NettyUtils.clientID(channel);
        boolean cleanSession = NettyUtils.cleanSession(channel);
        LOG.info("DISCONNECT client <{}> with clean session {}", clientID, cleanSession);
        ClientSession clientSession = m_sessionsStore.sessionForClient(clientID);
        clientSession.disconnect();

        m_clientIDs.remove(clientID);
        channel.close();

        LOG.info("DISCONNECT client <{}> finished", clientID, cleanSession);
    }

    @Override
    public void processConnectionLost(String clientID, boolean sessionStolen, Channel channel) {
        ConnectionDescriptor oldConnDescr = new ConnectionDescriptor(clientID, channel, true);
        m_clientIDs.remove(clientID, oldConnDescr);
        //If already removed a disconnect message was already processed for this clientID
        if (sessionStolen) {
            //de-activate the subscriptions for this ClientID
            ClientSession clientSession = m_sessionsStore.sessionForClient(clientID);
            clientSession.deactivate();
            LOG.info("Lost connection with client <{}>", clientID);
        }
    }

    /**
     * Remove the clientID from topic subscription, if not previously subscribed,
     * doesn't reply any error
     */
    @Override
    public void processUnsubscribe(Channel channel, UnsubscribeMessage msg) {
        List<String> topics = msg.topicFilters();
        int messageID = msg.getMessageID();
        String clientID = NettyUtils.clientID(channel);

        LOG.debug("UNSUBSCRIBE subscription on topics {} for clientID <{}>", topics, clientID);

        ClientSession clientSession = m_sessionsStore.sessionForClient(clientID);
        verifyToActivate(clientID, clientSession);
        for (String topic : topics) {
            boolean validTopic = SubscriptionsStore.validate(topic);
            if (!validTopic) {
                //close the connection, not valid topicFilter is a protocol violation
                channel.close();
                LOG.warn("UNSUBSCRIBE found an invalid topic filter <{}> for clientID <{}>", topic, clientID);
                return;
            }

            subscriptions.removeSubscription(topic, clientID);
            clientSession.unsubscribeFrom(topic);
        }

        //ack the client
        UnsubAckMessage ackMessage = new UnsubAckMessage();
        ackMessage.setMessageID(messageID);

        LOG.info("replying with UnsubAck to MSG ID {}", messageID);
        channel.writeAndFlush(ackMessage);
    }

    @Override
    public void processSubscribe(Channel channel, SubscribeMessage msg) {
        String clientID = NettyUtils.clientID(channel);
        LOG.debug("SUBSCRIBE client <{}> packetID {}", clientID, msg.getMessageID());

        ClientSession clientSession = m_sessionsStore.sessionForClient(clientID);
        verifyToActivate(clientID, clientSession);
        //ack the client
        SubAckMessage ackMessage = new SubAckMessage();
        ackMessage.setMessageID(msg.getMessageID());

        List<Subscription> newSubscriptions = new ArrayList<>();
        for (SubscribeMessage.Couple req : msg.subscriptions()) {
            QOSType qos = QOSType.valueOf(req.qos);
            Subscription newSubscription = new Subscription(clientID, req.topicFilter, qos);
            boolean valid = clientSession.subscribe(req.topicFilter, newSubscription);
            ackMessage.addType(valid ? qos : QOSType.FAILURE);
            if (valid) {
                newSubscriptions.add(newSubscription);
            }
        }

        //fire the publish
        for(Subscription subscription : newSubscriptions) {
            subscribeSingleTopic(subscription);
        }

        LOG.debug("SUBACK for packetID {}", msg.getMessageID());
        if (LOG.isTraceEnabled()) {
            LOG.trace("subscription tree {}", subscriptions.dumpTree());
        }
        channel.writeAndFlush(ackMessage);
    }

    private boolean subscribeSingleTopic(final Subscription newSubscription) {
        subscriptions.add(newSubscription.asClientTopicCouple());
        return true;
    }

    public void notifyChannelWritable(Channel channel) {
        String clientID = NettyUtils.clientID(channel);
        ClientSession clientSession = m_sessionsStore.sessionForClient(clientID);
        boolean emptyQueue = false;
        while (channel.isWritable()  && !emptyQueue) {
            AbstractMessage msg = clientSession.dequeue();
            if (msg == null) {
                emptyQueue = true;
            } else {
                channel.write(msg);
            }
        }
        channel.flush();
    }
}
