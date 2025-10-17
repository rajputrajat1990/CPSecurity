## Core Building Blocks

```
Cluster A ════════════════════► Cluster B
         [Cluster Link: "my-link"]
```


### Source Cluster

The cluster **originating the data**. Think of this as your production database that's currently handling writes.

**Example**: Your Kafka cluster in `us-east-1` where your order processing service writes messages.

### Destination Cluster

The cluster **receiving the replicated data**. This is where mirror topics live.

**Example**: Your DR cluster in `us-west-2` that receives copies of your production data.

### Source Topic

A **regular, writable topic** on the source cluster. Your applications produce to this topic.

**Key characteristic**: Applications can read from and write to it—it's a normal Kafka topic.

```bash
# On source cluster - normal topic behavior
kafka-console-producer --topic customer-orders --broker-list source:9093
# This works ✓
```


### Mirror Topic

A **read-only replica** of a source topic that lives on the destination cluster. This is the fundamental concept of cluster linking.

**Critical properties**:

- **Read-only**: You can consume from it, but you CANNOT produce to it
- **Byte-for-byte copy**: Messages, offsets, and partitions are identical to the source
- **Owned by the cluster link**: Created and managed by the link, not by standard topic commands

**Analogy**: Think of a mirror topic like a database read replica—it reflects the primary's state but doesn't accept writes directly.

```bash
# On destination cluster - mirror topic behavior
kafka-console-producer --topic customer-orders --broker-list dr:9093
# ERROR: Cannot produce to mirror topic ✗
```


## Data Flow Concepts

### Byte-for-Byte Replication

Messages are copied **exactly as they are**, including their exact offsets and partition assignments.

**Why this matters**: Traditional replication tools (like MirrorMaker) change offsets when copying messages. Cluster linking preserves them.

**Real-world impact**:

```
Source cluster:     Partition 0, Offset 1000 → "Order #12345"
Mirror topic:       Partition 0, Offset 1000 → "Order #12345"
                    ↑                ↑
                Same partition    Same offset
```

This means consumers can failover seamlessly—their offsets still work on the DR cluster.

### Mirroring Lag

The **delay** between when a message is written to the source topic and when it appears in the mirror topic.

**Measurement**: Number of messages or time delay.

**Example**:

```
Source topic:  Latest offset = 10,000
Mirror topic:  Latest offset = 9,950
Mirroring lag: 50 messages
```

**Why it exists**: Replication is asynchronous—there's network latency, processing time, etc.

**Monitoring command**:

```bash
kafka-mirrors --describe --link my-link --bootstrap-server dr:9093

# Output:
# TOPIC              LAG
# customer-orders    12
# payment-events     0
```


## Link Directionality

### Unidirectional Link

Data flows **one way only**: Source → Destination.  

**Visual**:

```
Source Cluster ────────► Destination Cluster
              one-way
```

**Use case**: Simple DR, migrations, data sharing.

### Bidirectional Link

Two separate cluster links configured so data **can flow both ways**.

**Important**: Requires **two cluster link objects**, one on each cluster.

**Visual**:

```
Cluster A ◄────────► Cluster B
      link-A-to-B     link-B-to-A
      (on B)          (on A)
```

**Use case**: Advanced DR with automated failback, active-active architectures.

### Link Mode

Defines the **operational mode** of a cluster link:

**DESTINATION** (default):

- Link is created on the destination cluster
- Destination "pulls" data from source
- Most common mode

**SOURCE**:

- Link is initiated from source cluster
- Source "pushes" data to destination
- Used when destination can't reach source (firewall restrictions)

**BIDIRECTIONAL**:

- Enables `reverse-and-start` and `truncate-and-restore` commands
- Both clusters can be source AND destination
- Required for automated failback operations


## Configuration Concepts

### Link Prefix

A string added to the **beginning of mirror topic names**.

**Configuration**: `cluster.link.prefix=usa_`

**Example**:

```
Source topic:  orders
Mirror topic:  usa_orders  (with prefix "usa_")
```

**Why use it**: When aggregating data from multiple regions, prevent name collisions:

```
Source cluster US:  orders → usa_orders (on destination)
Source cluster EU:  orders → eu_orders (on destination)
```


### Auto-Create Mirror Topics

Automatically creates mirror topics for **any matching topics** on the source cluster.

**Configuration**:

```properties
auto.create.mirror.topics.enable=true
auto.create.mirror.topics.filters={
  "topicFilters": [
    {"name": "*", "patternType": "LITERAL", "filterType": "INCLUDE"}
  ]
}
```

**Analogy**: Like setting up automatic photo backup—any new photo (topic) on your phone (source) automatically gets copied to the cloud (destination).

### Filter Types

Control **which topics** get auto-mirrored:

**INCLUDE filter**: Mirror topics matching this pattern

```json
{"name": "orders", "patternType": "PREFIXED", "filterType": "INCLUDE"}
// Mirrors: orders, orders-2024, orders-archive
```

**EXCLUDE filter**: DON'T mirror topics matching this pattern

```json
{"name": "secret", "patternType": "PREFIXED", "filterType": "EXCLUDE"}
// Excludes: secret-data, secret-logs, secret-keys
```

**Pattern Types**:

- **LITERAL**: Exact match only
- **PREFIXED**: Match anything starting with the name


## Consumer and Offset Concepts

### Consumer Offset Sync

Automatically copies **consumer group offsets** from source to destination.

**Configuration**:

```properties
consumer.offset.sync.enable=true
consumer.offset.sync.ms=30000  # Sync every 30 seconds
```

**Why critical for DR**: When consumers failover to the DR cluster, they pick up from where they left off.

**Example**:

```
Source cluster:
  Consumer group "payment-processor" at offset 5000

DR cluster (after sync):
  Consumer group "payment-processor" at offset 5000
  ↑
  Consumer can resume here after failover
```


### Consumer Group Filters

Specify **which consumer groups** to sync offsets for.

**Configuration**:

```json
{
  "groupFilters": [
    {"name": "prod-*", "patternType": "PREFIXED", "filterType": "INCLUDE"}
  ]
}
```

**Best practice**: Only sync consumer groups that will failover—exclude any groups running on the destination cluster.

### Offset Clamping

When consumer offsets are **adjusted down** after failover because the mirror topic doesn't have all the messages yet.

**Scenario**:

```
Source topic:     Messages up to offset 100
Mirror topic lag: Only has messages up to offset 90

Consumer A at offset 85: ✓ Can resume at 85 (messages exist)
Consumer B at offset 95: ✗ Clamped down to 90 (to avoid gaps)
```

**Why it happens**: Mirroring lag means some recent messages aren't on the DR cluster yet.

## Operational Commands and States

### Failover

Converts a mirror topic into a **writable, regular topic** immediately—even if source is unreachable.

**Command**:

```bash
kafka-mirrors --failover customer-orders --link my-link --bootstrap-server dr:9093
```

**What happens**:

- Mirror topic becomes writable
- Applications can now produce to it
- Mirroring relationship stops

**Use case**: Disaster recovery—source cluster is down, need to fail over NOW.

### Promote

Similar to failover, but **checks lag first** to ensure zero data loss.

**Command**:

```bash
confluent kafka mirror promote customer-orders --link my-link
```

**What happens**:

- Verifies mirroring lag = 0
- Verifies consumer offset lag = 0
- THEN converts to writable topic
- Fails if source unreachable or lag > 0

**Use case**: Planned migrations—source is healthy, want to guarantee no data loss.

### Truncate-and-Restore

**Reverses roles**: makes the original source topic into a mirror of the DR cluster.

**Requirements**:

- BOTH clusters must be in KRaft mode
- Link must be bidirectional
- ⚠️ **Deletes any divergent data** on the original primary

**Command**:

```bash
confluent kafka mirror truncate-and-restore customer-orders \
  --link my-bidirectional-link
```

**What happens**:

1. Truncates (deletes) divergent messages on original primary
2. Converts original primary's topic to a mirror
3. Starts fetching from the DR cluster

**Visual**:

```
Before:  Primary (source) ──► DR (mirror)
After:   Primary (mirror) ◄── DR (source)
```


### Reverse-and-Start

**Swaps source and mirror roles** while preserving all data.

**Requirements**:

- BOTH clusters must be in KRaft mode
- Link must be bidirectional

**Command**: 

```bash
confluent kafka mirror reverse-and-start customer-orders \
  --link my-bidirectional-link
```

**What happens**: 

1. Source topic becomes read-only (stops accepting writes)
2. Mirror topic catches up completely
3. Roles swap: mirror becomes source, source becomes mirror
4. New source accepts writes again

**Use case**: Automated failback after DR event. 

### Reverse-and-Pause

Same as `reverse-and-start` but leaves the new mirror in **PAUSED state** (not actively syncing). 

**Use case**: You want to reverse roles but manually control when mirroring starts. 

## Mirror Topic States

Mirror topics go through different **lifecycle states**: 

### ACTIVE

Mirror topic is **actively replicating** from source. 

```bash
# Normal steady-state operation
STATE: ACTIVE
LAG: 5
```


### PAUSED

Mirroring is **temporarily stopped** (can be resumed). 

```bash
# Manually paused for maintenance
kafka-mirrors --pause customer-orders --link my-link
STATE: PAUSED
```


### STOPPED

Mirror topic has been **converted to a regular topic** (via failover or promote). 

```bash
# After running failover
STATE: STOPPED
# Topic is now writable
```


### FAILED

Something went wrong; mirror topic is in an **error state**. 

**Common causes**: 

- Source cluster unreachable
- Permission issues
- Invalid message format


### PENDING_STOPPED

Transitional state during **reverse operations**—waiting for final metadata sync. 

### PENDING_SYNCHRONIZE

Transitional state during **reverse operations**—mirror catching up to source before role swap. 

## ACL and Security Concepts

### ACL Sync

Automatically replicates **Access Control Lists** (permissions) from source to destination. 

**Configuration**: 

```properties
acl.sync.enable=true
```

**Limitation**: Cannot be used with link prefixing. 

**What syncs**: Permissions on topics (who can read/write/configure). 

## Advanced Concepts

### Source-Initiated Cluster Link

The **source cluster** creates the link and pushes data to destination. 

**Use case**: Firewall prevents destination from reaching source, but source can reach destination. 

**Visual**:

```
Firewall blocks ←
Source ──────────► Destination
       can connect
```


### Hybrid Cloud

Linking an **on-premises Confluent Platform cluster** with a **Confluent Cloud cluster**.  

**Key benefit**: Source-initiated links work without firewall holes (outbound connections only). 

### Topic Chaining

Creating a mirror of a mirror. 

**Example**:

```
Cluster A: orders (source)
  ↓ link 1
Cluster B: orders (mirror)
  ↓ link 2
Cluster C: orders (mirror of mirror)
```

**Limitation**: Auto-create with prefixing doesn't support chaining. 

### Aggregation

Multiple source clusters replicating to **one central destination cluster**.  

**Pattern**: 

```
US Cluster ──► orders → usa_orders ──┐
                                      ├──► Central Cluster
EU Cluster ──► orders → eu_orders ───┘
```

**Key technique**: Use link prefixes to avoid name collisions. 

## Performance and Scaling Terms

### Fetch Size

How much data the cluster link **fetches in each request** from the source. 

**Configuration**: `fetch.max.bytes`

**Tuning**: Increase for better throughput, decrease to reduce memory usage. 

### Fetcher Threads

Number of **parallel threads** pulling data from source topics. 

**Configuration**: Depends on partition count. 

**Scaling**: More partitions = more parallelism. 

### CKU (Confluent Kafka Unit)

**Capacity unit** in Confluent Cloud—defines throughput limits. 

**Cluster linking bandwidth**: 

- Source: 150 MB/s egress per CKU
- Destination: 50 MB/s ingress per CKU

**Scaling**: Add more CKUs to increase cluster linking throughput. 

## Metadata Concepts

### Metadata Max Age

How often the cluster link **refreshes cluster metadata**. 

**Configuration**: `metadata.max.age.ms=300000` (5 minutes default) 

**Impact on auto-create**: Determines how frequently new topics are discovered and auto-mirrored. 

### Mirror Start Offset Spec

Where mirroring **begins** when a mirror topic is created.  

**Options**: 

- **Earliest**: Mirror all historical data
- **Latest**: Mirror only new messages from now on
- **Specific offset**: Start from a particular offset


## DR-Specific Terminology

### RPO (Recovery Point Objective)

Maximum **acceptable data loss** measured in time. 

**In cluster linking**: Determined by mirroring lag.  

**Example**: If lag is 30 seconds, RPO = 30 seconds of potential data loss.  

### RTO (Recovery Time Objective)

Time to **restore operations** after a disaster. 

**In cluster linking**: Time to run failover + restart applications.  

**Example**: If failover takes 2 minutes and app restarts take 3 minutes, RTO = 5 minutes.  

### Active-Passive Architecture

**One cluster** serves traffic (active), other is on standby (passive).  

### Active-Active Architecture

**Both clusters** serve traffic simultaneously.  