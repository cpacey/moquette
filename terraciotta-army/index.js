'use strict';

const mqtt = require('mqtt');
const uuid = require('uuid');

process.on('SIGINT', () => process.exit(130));

const mqttUrl = process.env.MQTT_TEST_CLIENT_MQTT_URL;
if (!mqttUrl) {
    console.error('MQTT_TEST_CLIENT_MQTT_URL not defined');
    process.exit(1);
}

const numberOfClients = parseInt(process.env.MQTT_TEST_CLIENT_WORKERS);
if (numberOfClients < 1) {
    console.error('MQTT_TEST_CLIENT_WORKERS must be a positive number');
    process.exit(1);
}

const stats = {
    sent: 0,
    received: 0,
    latencyTotal: 0,
    spurious: 0
}

let connectionCount = 0;
setInterval(
    () => {
        console.log(
            'cn:', connectionCount,
            ', s:', stats.sent,
            ', r:', stats.received,
            ', l(a):', (stats.latencyTotal / stats.received),
            ', ?:', stats.spurious
        );
    },
    1000
);

const generation = uuid.v4();

for (let deviceId = 0; deviceId < numberOfClients; deviceId++) {
    const topic = `cti.testing.dev/devices/${deviceId}`;

    let sendMessageIntervalId;
    function startSendingMessages() {
        sendMessageIntervalId = setInterval(
            () => {

                const data = {
                    generation,
                    date: new Date().toISOString()
                };
                const payload = JSON.stringify(data);

                client.publish(
                    topic,
                    payload
                );

                stats.sent += 1;

                /*console.log(`${Date.now()} - ${i} - Sent message`)*/
            },
            4000 + (Math.random() * 2000)
        );
    }

    let client;
    let connected = false;
    function wireUpClient() {
        /*console.log(`${Date.now()} - ${i} - Wiring up client`);*/
        client = mqtt.connect(mqttUrl);
        client.once('connect', () => startSendingMessages());
        client.on('connect', () => {
            if (!connected) {
                connectionCount += 1;
                connected = true;
            }
        });
        client.on('close', () => {
            if (connected) {
                connectionCount -= 1;
                connected = false;
            }
        });
        client.once('error', err => {
            console.error('Error occurred');
            console.error(err);
            process.exit(2);
        });
        client.subscribe(topic, err => {
            if (err) {
                console.error('Error subscribing');
                console.error(err);
                process.exit(3);
            }
            /*console.log(`${Date.now()} - ${i} - Subscribed`);*/
        } );
        client.on('message', function(incomingTopic, message) {
            /*console.log(`${Date.now()} - ${i} - Received message`);*/

            if (incomingTopic === topic) {
                let payload;

                try {
                    payload = JSON.parse(message.toString());
                } catch (e) {
                    return;
                }

                if (payload.generation !== generation) {
                    return;
                }

                const sentAt = new Date(payload.date.toString());
                const diff = new Date() - sentAt;

                if (isNaN(diff)) {
                    stats.spurious += 1;
                } else {
                    stats.received += 1;
                    stats.latencyTotal += diff;
                }
            } else {
                console.error('Unexpected incoming message');
                console.error(err);
                process.exit(4);
            }
        } );
    }

    wireUpClient();

    setInterval(
        () => {
            if (Math.random() > 0.9) {
                clearInterval(sendMessageIntervalId);
                client.end();
                wireUpClient();
            }
        },
        10000
    );
}
