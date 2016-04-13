# An Experiment with Kafka and MQTT

We've been using [Mosquitto](http://mosquitto.org/) as our MQTT server for our
work so far.  Mosquitto has done the job so far, but we're concerned about it's
scalability.  Specifically, it only scales up, and we're confident it will fall
over long before we reach 1000000 connections.  There are options for dealing
with this, e.g. sharding with standby, but they come with a lot of design and
operational complexities.  It would be great if we could instead scale MQTT
_out_ instead of _up_ - just like we do with our web apps.

Added to this, we also have an problem with our cloud services that consume data
coming in from MQTT - because of MQTT's Pub/Sub nature, attempts to scale out
said cloud services results in each node getting all messages.  This is not What
we want - instead, we want to scale out the cloud services and have only one
node process the message - and we'd like that to be reliable and fault-tolerant
(e.g. if a node dies while processing a message, then the message will be
processed by another node).

At first glance, [Kafka](http://kafka.apache.org/) feels like it could help
here.  It scales out, and it supports both Pub/Sub and Message Queuing (through
groups).  So then we wondered: can we put a reasonable MQTT front-end on Kafka,
and would it perform sufficiently?

Along the way, we made a few realizations that simplified things:

1. We don't need QoS 2.  (In general, we're skeptical of delivering anything
"exactly once" in a distributed system.)
1. We don't need QoS 1.  This is a result of how we use MQTT - specifically,
since the cloud services will use Kafka, only the IOT devices will use the
MQTT interface, and the nature of the messages we send does not require that
said messages are delivered later if the device is not currently connected.
This is a direct result of...
1. We need retained messages.  We've fallen into using retained messages instead
of QoS 1 (for better or worse).  Our devices generally don't care about the
contents of _all_ messages on a topic, but do often care a great deal about the
contents of the _last_ message on a topic.  Retained messages gets us this.
1. We don't need wildcards (+/#).  Again, only the IOT devices use the
MQTT interface, and they know the exact topic(s) they want to listen to.  We've
been using the wildcards for cloud services, but those will be using Kafka,
rather than MQTT.

Interestingly, there's at least one commonality with AWS IoT's [list of
limitations](http://docs.aws.amazon.com/iot/latest/developerguide/protocols.html).

## Technical Details

### Changes to Moquette

At a high-level, the changes are simple: we replaced the existing
[ProtocolProcessor](https://github.com/andsel/moquette/blob/master/broker/src/main/java/io/moquette/spi/impl/ProtocolProcessor.java)
with a new implementation.  This essentially kept Moquette's protocol handling
while replacing the broker itself.

Moquette has abstractions for [message](https://github.com/andsel/moquette/blob/master/broker/src/main/java/io/moquette/spi/IMessagesStore.java) and [session](https://github.com/andsel/moquette/blob/master/broker/src/main/java/io/moquette/spi/ISessionsStore.java)
storage, but these were a poor fit for our use-case. Specifically, Kafka ends up
owning message storage, and is too opinionated to be a good fit.  Session
storage would also need to be managed in an external datastore (Couchbase would
likely have been our choice), which suggested an async interface to me.
Regardless, neither of these is necessary for QoS 0.

Given the above realizations, and in the interest of quick experimentation, we
removed most features from the new Kafka-based ProtocolProcessor, including:

1. Security/auth
1. Qos 1 & 2 (session storage, message storage, queuing, etc)
1. Will Topics
1. Retained messages (while we will need these, removing them helped us get moving,
    and our time-box elapsed before we got back to it)

It's also worth noting that I did not modify the existing test suite, which of
course fails miserably now.  I did not create or maintain my own tests due to
the experimental nature of this exercise - the investment in tests did not seem
like a good investment given the timeframe and focus on scale/performance (which
I've found requires much more complicated automated tests).

### Topic Names

MQTT topic names can contain any UTF-8 character - although some have special
meaning (/, +, #), any UTF-8 character is allowed.  By comparison, Kafka only
allows [0-9A-Za-z\\-\_\\.].  As such, pretty much any MQTT topic will be
rejected by Kafka (since / is not allowed).  This prototype lazily converts '/'
to '\_', and simply doesn't work if your topic name doesn't match
[0-9A-Za-z\\-\\.].

Long term, I think a something like Base64 encoding the topics would do.  Base64
itself isn't an option due to the use of '+' and '/', but we could convert those
to '-' and '.' for the same effect.  This would also leave '\_' to be used as
the topic level separator, in place of '/'.  In this model, each level in the
MQTT topic is ~Base64 encoded and separated by '\_', which should allow full use
of the MQTT topic name space while also allowing the Kafka consumers to consume
the messages using Kafka wildcards (regexes).

### Publishing

Publishing is pretty simple.  There's a single [KafkaProducer](https://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html) that is used to publish all messages to Kafka.
Publishing is done on the Netty thread that processed the incoming PUBLISH
message.  The upside of this is simplicity.  The downsides are:

1. KafkaProducer.send, despite being described in the docs as "Asynchronously
send a record to a topic", can block, sometimes for greater than 10s.
1. When KafkaProducer.send blocks, all Netty activity on that thread is blocked.
This includes all incoming message handling for clients being handled on that
thread, and possibly also all outgoing messages.
1. While KafkaProducer.send is blocked on one thread, it usually blocks other
threads that call KafkaProducer.send.  The most common example is that it would
block waiting for metadata.  This means that when KafkaProducer.send blocks, it
tends to back up the whole system.

This could be mitigated by moving KafkaProducer.send to it's own thread (or
threadpool).  This would unblock Netty.  However, whenever KafkaProducer.send is
blocked, this would necessitate either:

1. Buffering incoming messages.  What do we do when this gets too big?
1. Dropping incoming messages.
1. Some combination of the above.

Unfortunately, one of the most common causes of KafkaProducer.send blocking was
due to topic auto-creation.  In this case, KafkaProducer.send is delayed getting
the metadata for the topic, since the Kafka nodes need to create the topic
partitions and do leader elections.  This is unfortunate since topic dynamic
topic creation is a very common use-case in MQTT.  As an example, the benchmarks
we took (see below) would cause havoc if the topics were not first created in
Kafka.

### Consuming

The initial pass gave each incoming MQTT connection its own thread running [KafkaConsumer](https://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html) polling loop.  This was done for simplicity, but scaled
very poorly (surprise!).  It worked for 5-10k clients, but everything was slow
and when those clients disconnected, the server spent _minutes_ cleaning up all
the threads and KafkaConsumers.

The second (and final) pass uses a single thread running a KafkaConsumer polling
loop.  We keep a Map of topics to clients, and when a message from in from Kafka
we simply lookup all the clients of the topic and send the message.

We consider using a pool of threads instead of a single thread, but there were
never any indications that we had hit the limits of the single thread.  Even
with 25k active clients and ~8k message per second, CPU sampling showed that all
Netty threads were busier than the single consumer thread.

It's worth noting that, similar to `KafkaProducer.send`,
`KafkaConumser.poll(long timeout)` can block for much longer than `timeout`.
This primarily happens because it's taking a long time to read metadata.  Unlike
`KafkaProducer.send`, however, `KafkaConumser.poll(long timeout)` doesn't appear
to block due to new/unknown topics.

The biggest difficulty with consuming is managing subscriptions.
`KafkaConsumer` has no facility for modifying subscriptions, only for
setting/replacing/overriding subscriptions.  This means that we need to do a lot
of bookkeeping around subscriptions.  The primary difficulty here is that
`KafkaConsumer` is not thread-safe; specifically, changing subscriptions during
a `poll` is problematic.  This means that the Netty threads (which are handling
the SUBSCRIBE messages) and the `KafkaConsumer` polling thread both need to use
the list of subscribed topics, but not at the same time - and I can't spot how
to do that without _some_ kind of locking (even if it's on a work queue, a la Go
channels).  This is worth noting because early on in the design, the critical
section between Netty and `KafkaConsumer` threads was much larger than it is
now, and appeared to causing intermittent-but-serious issues.  Indeed, reducing
the size of the critical section(s) remove several bugs.

## Load Testing

The reason for this experiment is to see whether the approach would scale.
Ideally, this would have included testing scale out on the MQTT broker, with
e.g. Nginx load balancing.  Unfortunately, our attempts to get things running
on our development hardware cluster were not successful within the timebox.
As such, all load data is from running on my development machine.

The server was always run using VisualVM, to allow inspection and profiling of
the server application.  Debugging was sometimes enabled, depending on whether
was expecting the test run to produce an error state worth inspecting.

Kafka was running as a 3-broker cluster with 1 Zookeeper node.  These 4 nodes
were running as docker containers on my desktop, and can be run using the
docker-compose.yml file in the root of this repo.

Initial (development) load testing was done using [emqtt_benchmark](https://github.com/emqtt/emqtt_benchmark).  This was useful in
finding early limitations (e.g. thread-per-client model @ 10k clients), but
doesn't behave much like an IOT device.

Later testing was done with a custom-built tool (terraciotta-army).  The version
in this repo is heavily modified from https://github.com/CanTireInnovations/terraciotta-army.
This tool is intended to better mimic the behaviour of an army of IOT devices.
The modifications in this repo are to better capture information about the
messages, including send/receive counts (to ensure messages aren't getting lost)
and message latency (i.e. time between when the message was published and when
it arrived at the subscriber).

Unfortunately, I was not careful enough and did not get recordings of the data.
However, the inclusion of all the code in this repo can allow for reproduction
of the data if necessary.

### emqtt_benchmark Load Tests

These tests used the emqtt_benchmark app.

I was initially able to connect 10k subscribers and 15k publishers @ 1msg/5sec.
In this state, the emqtt_benchmark was consuming large amounts of CPU and
causing my whole desktop to be slow.

I was also able to connect ~38k publishers @1msg/5sec (with 0 subscribers).  The
server was barely taxed, and I only stopped at ~38k publishers because of a hard
40k limit on file descriptors.

### terraciotta-army Load Tests

I was able to get ~25k fake IOT devices - so 25k connections, each connection
subscribed to one topic and publishing to that same topic @ 1msg/[4-6]sec.  The
fake IOT devices were spread across 4 terraciotta-army processes, 2 @ 7500
connections, 2 @ 5000 connections.  This was primarily done because I found that
7500 connections would consume a single core (terraciotta-army is written in
Node.js, which is inherently single-threaded) but run as expected (e.g. output
every second), whereas 8000 connections would run slowly (e.g. long pauses
between output, etc).

In this state, with 25k fake IOT devices and all of the server, clients, and
Kafka cluster running on the same desktop, I was bound by total CPU.  There were
no apparent limitations on the server itself, and I believe it could have
handled far more fake IOT devices, but the available hardware did not allow
that.  This situation was repeated many times.  I made several attempts to
increase the number of clients, but at 26k fake IOT devices, the whole desktop
was much slower and the terraciotta-army processes started dropping connections.

The primary difference between test runs was the latency in message delivery.  I
could discern no pattern to it, but often the initial message latency would be
very high, e.g. with the first batch of messages taking > 10s to deliver;
latency would almost always come down after this, but would take a long time
since the latency  is an average across the lifetime of the process (so spikes
take a long time to go away).  However, there were many test runs where latency
was immediately low and would stay low; during these runs, latency was usually
175-350ms average, and would stay that way for long runs (up to dozens of
minutes).

## Running the Test yourself

### Building the Server

Although there's a Dockerfile in this repo, it just copies artifacts from the
host, so you actually have to build the server yourself first :(.

1. Install JDK (I used Oracle JDK 8)
1. Install maven
1. `mvn package -DskipTests`

### Run the System

There's a docker-compose file that will build the server docker image and then
run Zookeeper, Kafka, and the server:

```bash
docker-compose up -d
```

### Run terraciotta-army

terraciotta-army takes arguments as environment variables (half due to laziness,
half due to our love of docker).  The two args are:

1. `MQTT_TEST_CLIENT_MQTT_URL`, e.g. `tcp://localhost:1883`
1. `MQTT_TEST_CLIENT_WORKERS` - number of fake IOT devices

If you've run the docker-compose file above, you can start an IOT army of one with:

```bash
MQTT_TEST_CLIENT_MQTT_URL=tcp://localhost:1883 MQTT_TEST_CLIENT_WORKERS=1 node index.js
```

The output will be as follows:

```bash
cn: 45 , s: 35 , r: 34 , l(a): 190.44117647058823 , ?: 0
cn: 51 , s: 40 , r: 40 , l(a): 186.975 , ?: 0
cn: 51 , s: 55 , r: 52 , l(a): 167.44230769230768 , ?: 0
```

* `cn` - Number of active connections
* `s` - Total number of messages sent
* `r` - Total number of sent messages that have been received
* `l(a)` - Average message latency (i.e. time between when the message is sent and received)
* `?` - Ignore this (was for debugging)

## Lessons Learned

These are things we learned during this process that we don't want to forget.

1. On using Kafka wildcards, e.g. for '+/#': [KafkaConsumer.subscribe(java.util.regex.Pattern,...)](https://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html)) states
that "The pattern matching will be done periodically against topics existing
at the time of check", but the frequency of "periodically" is not clearly
stated.  Empirical tests suggest that the check is done when metadata is
updated, and so can be controlled by the "metadata.max.age.ms" ([docs](http://kafka.apache.org/documentation.html#newconsumerconfigs)).
1. The first publish to a new, auto-created Kafka topic will almost certainly
block and fail.
1. Kafka is easier to debug/trace once you correctly configure log4j :).  (
Otherwise, it can be a silently broken black box.)

---

# Original README follows

---

## What is Moquette?

[![Build Status](https://api.travis-ci.org/andsel/moquette.svg?branch=master)](https://travis-ci.org/andsel/moquette)

* [Documentation reference guide] (http://andsel.github.io/moquette/) Guide on how to use and configure Moquette
* [Google Group] (https://groups.google.com/forum/#!forum/moquette-mqtt) Google Group to participate in development discussions.
Moquette aims to be a MQTT compliant broker. The broker supports QoS 0, QoS 1 and QoS 2.

Its designed to be evented, uses Netty for the protocol encoding and decoding part.

## Embeddable

[Freedomotic] (http://www.freedomotic.com/) Is an home automation framework, uses Moquette embedded to interface with MQTT world.
Moquette is also used into [Atomize Spin] (http://atomizesoftware.com/spin) a software solution for the logistic field.
Part of moquette are used into the [Vertx MQTT module] (https://github.com/giovibal/vertx-mqtt-broker-mod), into [MQTT spy](http://kamilfb.github.io/mqtt-spy/)
and into [WSO2 Messge broker] (http://techexplosives-pamod.blogspot.it/2014/05/mqtt-transport-architecture-wso2-mb-3x.html).

## 1 minute set up
Start play with it, download the self distribution tar from [BinTray](https://bintray.com/artifact/download/andsel/generic/distribution-0.8-bundle-tar.tar.gz) ,
the un untar and start the broker listening on 1883 port and enjoy!
```
tar zxf distribution-0.8-bundle-tar.tar.gz
cd bin
./moquette.sh
```

Or if you are on Windows shell
```
 cd bin
 .\moquette.bat
 ```

## Embedding in other projects
To embed Moquette in another maven project is sufficient to include a repository and declare the dependency:

```
<repositories>
  <repository>
    <id>bintray</id>
    <url>http://dl.bintray.com/andsel/maven/</url>
    <releases>
      <enabled>true</enabled>
    </releases>
    <snapshots>
      <enabled>false</enabled>
    </snapshots>
  </repository>
</repositories>
```

Include dependency in your project:

```
<dependency>
      <groupId>io.moquette</groupId>
      <artifactId>moquette-broker</artifactId>
      <version>0.8</version>
</dependency>
```

## Build from sources

After a git clone of the repository, cd into the cloned sources and: `mvn clean package`.
In distribution/target directory will be produced the selfcontained tar for the broker with all dependencies and a running script.
