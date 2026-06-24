# Salesforce CDC & Platform Events — Java POC

Enterprise-grade Java client for consuming **Change Data Capture (CDC)** events and **Platform Events**
from the Salesforce Streaming API (CometD / Bayeux Protocol via HTTP Long Polling).

---

## Project Structure

```
streaming-api-poc/
├── pom.xml                                         Maven build descriptor
├── local.properties.template                       Credentials template (copy → local.properties)
├── logs/                                           Runtime logs & checkpoint file (git-ignored)
└── src/
    ├── main/
    │   ├── java/com/enterprise/salesforce/
    │   │   ├── SalesforceStreamingClient.java      ← Main entry point
    │   │   ├── auth/
    │   │   │   └── SalesforceAuthManager.java      OAuth 2.0 token lifecycle
    │   │   ├── checkpoint/
    │   │   │   └── ReplayIdCheckpointStore.java    Durable replay ID persistence
    │   │   ├── consumer/
    │   │   │   ├── StreamingApiSubscriptionManager.java  EMP Connector + Queue + Pool
    │   │   │   └── StreamingEvent.java             Immutable event envelope
    │   │   └── processing/
    │   │       └── EventProcessors.java            CDC + Platform Event handlers
    │   └── resources/
    │       └── logback.xml                         Logging config
    └── test/
        └── java/com/enterprise/salesforce/
            ├── ReplayIdCheckpointStoreTest.java
            └── StreamingEventTest.java
```

---

## Prerequisites

| Requirement | Version |
|---|---|
| Java JDK | 17+ |
| Maven | 3.8+ |
| Salesforce org | Developer Edition or Sandbox |
| Connected App | OAuth enabled (see below) |

---

## Quick Start

### 1. Clone / open in IntelliJ

Open IntelliJ → **File → Open** → select this folder.
IntelliJ will detect `pom.xml` and import the Maven project automatically.

> If prompted: **Trust project** → **Open as Maven project**

### 2. Configure credentials

```bash
cp local.properties.template local.properties
# Edit local.properties with your Salesforce credentials
```

### 3. Configure IntelliJ Run Configuration

A pre-built run configuration is included at `.idea/runConfigurations/SalesforceStreamingClient.xml`.

**Edit it before running:**

1. In IntelliJ: **Run → Edit Configurations → SalesforceStreamingClient**
2. Replace the `-Dsf.*` VM Options placeholders with your actual values from `local.properties`

> **Password format:** concatenate your Salesforce password + security token with NO separator.  
> Example: password=`MyPass123`, token=`ABCXYZ` → enter `MyPass123ABCXYZ`

### 4. Enable CDC and/or create the Platform Event in Salesforce Setup

See `Salesforce_CDC_Platform_Events_Configuration_Guide.docx` for step-by-step instructions.

**TL;DR:**
- CDC: Setup → Quick Find: "Change Data Capture" → move Account to Selected Entities → Save
- Platform Event: Setup → Quick Find: "Platform Events" → New → Label: "My Event" → Save → add custom fields

### 5. Run

Click **Run ▶ SalesforceStreamingClient** in IntelliJ (or via Maven):

```bash
mvn package -DskipTests
java -jar target/streaming-api-poc-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  -Dsf.loginUrl=https://login.salesforce.com \
  -Dsf.clientId=YOUR_CLIENT_ID \
  -Dsf.clientSecret=YOUR_CLIENT_SECRET \
  -Dsf.username=your@org.com \
  -Dsf.password=yourPasswordPlusToken \
  -Dsf.checkpointFile=./logs/replay-checkpoints.properties
```

### 6. Publish a test event

Paste this in **Salesforce Developer Console → Debug → Execute Anonymous**:

```apex
MyEvent__e evt = new MyEvent__e();
evt.EventType__c     = 'ORDER_CREATED';
evt.SourceSystem__c  = 'ERP_TEST';
evt.CorrelationId__c = '550e8400-e29b-41d4-a716-446655440000';
evt.Payload__c       = '{"orderId":"ORD-001","amount":1500.00}';
Database.SaveResult sr = EventBus.publish(evt);
System.debug(sr.isSuccess() ? 'Published OK' : sr.getErrors());
```

You should see the event logged in the Java console within ~1 second.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     SalesforceStreamingClient                        │
│  (OAuth → BayeuxHandshake → Subscribe → JVM Shutdown Hook)          │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                ┌──────────▼──────────┐
                │  SalesforceAuthMgr  │  OAuth token + refresh
                └──────────┬──────────┘
                           │ bearerToken()
                ┌──────────▼──────────────────────┐
                │  StreamingApiSubscriptionManager │
                │                                  │
                │  EMP Connector (CometD/Bayeux)   │
                │  ──────────────────────────────  │
                │  CometD I/O Thread               │
                │      │ offer() — non-blocking    │
                │      ▼                           │
                │  BlockingQueue (cap: 10,000)     │
                │      │ poll(500ms)               │
                │      ▼                           │
                │  Dispatcher Thread               │
                │      │ submit()                  │
                │      ▼                           │
                │  ExecutorService (4 workers)     │
                └──────────┬───────────────────────┘
                           │
                ┌──────────▼──────────┐
                │  RoutingEventProc   │
                │  /data/*  → CDC     │
                │  /event/* → PlatEvt │
                └──────────┬──────────┘
                           │
                ┌──────────▼──────────┐
                │ ReplayIdCheckpoint  │  Checkpoint after enqueue
                │ Store (file/Redis)  │  → at-least-once delivery
                └─────────────────────┘
```

---

## Channels Configured

| Channel | Type | Trigger |
|---|---|---|
| `/data/AccountChangeEvent` | CDC | Any Account create/update/delete/undelete |
| `/event/MyEvent__e` | Platform Event | Explicit publish via Apex / REST API |

Add more channels to the `CHANNELS` list in `SalesforceStreamingClient.java`.

---

## Replay ID Behaviour

| Value | Constant | Behaviour |
|---|---|---|
| `-1` | `REPLAY_FROM_TIP` | New events only (default for first run) |
| `-2` | `REPLAY_ALL` | Replay all events in 72-hour retention window |
| `N` | Checkpointed | Resume from event N+1 (used automatically after first run) |

Delete `logs/replay-checkpoints.properties` to reset to tip on the next run.

---

## Running Tests

```bash
mvn test
```

---

## Production Checklist

- [ ] Replace file-based checkpoint store with Redis / JDBC for multi-instance deployments
- [ ] Switch OAuth flow from Username-Password to **JWT Bearer** (no user password in config)
- [ ] Route `processEvent()` dead-letter path to a dead-letter queue (Kafka / SQS)
- [ ] Add Micrometer metrics: queue depth, events/sec, processing latency
- [ ] Lock Connected App IP range to server egress IPs (remove IP relaxation)
- [ ] Set `WORKER_THREAD_COUNT` based on downstream I/O profiling
- [ ] Add `/data/ChangeEvents` firehose subscription for full CDC coverage
