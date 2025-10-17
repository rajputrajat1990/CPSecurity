## The Scenario Setup

**Your Infrastructure**:

**Primary Cluster** (Active - ZooKeeper-based)

- Location: **AWS us-east-1** (Virginia)
- Bootstrap servers: `primary-kafka-1.us-east-1:9093,primary-kafka-2.us-east-1:9093,primary-kafka-3.us-east-1:9093`
- Cluster ID: `primary-cluster-east`

**DR Cluster** (Passive - ZooKeeper-based)

- Location: **AWS us-west-2** (Oregon)
- Bootstrap servers: `dr-kafka-1.us-west-2:9093,dr-kafka-2.us-west-2:9093,dr-kafka-3.us-west-2:9093`
- Cluster ID: `dr-cluster-west`

**Critical Topics**:

- `customer-orders` - Your e-commerce orders
- `payment-events` - Payment transaction events
- `inventory-updates` - Real-time inventory changes

**Applications Running**:

- Order processing service (produces to `customer-orders`)
- Payment processor (produces to `payment-events`, consumes from `customer-orders`)
- Inventory service (produces to `inventory-updates`)


## Phase 1: Steady-State Setup (Already Done)

Before disaster strikes, you've already configured this:

### Step 1.1: Create Cluster Link on DR Cluster

```bash
# On DR cluster (us-west-2)
cat > cluster-link-config.properties <<EOF
bootstrap.servers=primary-kafka-1.us-east-1:9093,primary-kafka-2.us-east-1:9093
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="primary-password";
consumer.offset.sync.enable=true
consumer.offset.sync.ms=30000
EOF

kafka-cluster-links --create \
  --link primary-to-dr-link \
  --config-file cluster-link-config.properties \
  --bootstrap-server dr-kafka-1.us-west-2:9093 \
  --command-config dr-admin.properties
```


### Step 1.2: Create Mirror Topics on DR Cluster

```bash
# Create mirror topics for all critical topics
kafka-mirrors --create \
  --mirror-topic customer-orders \
  --link primary-to-dr-link \
  --bootstrap-server dr-kafka-1.us-west-2:9093 \
  --command-config dr-admin.properties

kafka-mirrors --create \
  --mirror-topic payment-events \
  --link primary-to-dr-link \
  --bootstrap-server dr-kafka-1.us-west-2:9093 \
  --command-config dr-admin.properties

kafka-mirrors --create \
  --mirror-topic inventory-updates \
  --link primary-to-dr-link \
  --bootstrap-server dr-kafka-1.us-west-2:9093 \
  --command-config dr-admin.properties
```


### Step 1.3: Verify Mirror Topics Are Syncing

```bash
# Check lag on mirror topics
kafka-mirrors --describe \
  --link primary-to-dr-link \
  --bootstrap-server dr-kafka-1.us-west-2:9093 \
  --command-config dr-admin.properties

# Output you want to see:
# TOPIC              STATE      SOURCE TOPIC       LAG
# customer-orders    ACTIVE     customer-orders    0
# payment-events     ACTIVE     payment-events     0
# inventory-updates  ACTIVE     inventory-updates  0
```

At this point, you have **steady-state replication running**. Your DR cluster is quietly receiving all data from production.

## Phase 2: DISASTER! us-east-1 Region Goes Down

**Friday, 3:47 PM** - AWS us-east-1 region experiences a total outage. Your monitoring alerts go crazy:

```
CRITICAL: Primary Kafka cluster unreachable
CRITICAL: Order processing service down
CRITICAL: Payment processor disconnected
```

Your applications can't reach the primary cluster. Data is piling up. You need to **fail over to DR immediately**.

## Phase 3: Execute Failover to DR (us-west-2)

### Step 3.1: Confirm Primary is Down

Try to reach primary cluster:

```bash
# This will timeout/fail
kafka-topics --list \
  --bootstrap-server primary-kafka-1.us-east-1:9093 \
  --command-config primary-admin.properties

# Connection refused or timeout
```

Primary is definitely down. Time to fail over.

### Step 3.2: Execute Failover Command on DR Cluster

Run the **failover command** on the DR cluster:

```bash
# Connect to DR cluster operations server
ssh ops-server-us-west-2

# Failover ALL critical topics at once
kafka-mirrors --failover \
  customer-orders \
  payment-events \
  inventory-updates \
  --link primary-to-dr-link \
  --bootstrap-server dr-kafka-1.us-west-2:9093 \
  --command-config dr-admin.properties
```

**What just happened**:

- The mirror topics `customer-orders`, `payment-events`, `inventory-updates` are now **regular, writable topics** on the DR cluster
- They're no longer mirrors—they're the source of truth
- Your DR cluster is now your production cluster

**Output you'll see**:

```
Successfully failed over topic: customer-orders
Successfully failed over topic: payment-events
Successfully failed over topic: inventory-updates
```


### Step 3.3: Verify Topics Are Writable

Check that topics are now writable:

```bash
kafka-mirrors --describe \
  --link primary-to-dr-link \
  --bootstrap-server dr-kafka-1.us-west-2:9093 \
  --command-config dr-admin.properties

# Output should show STOPPED state (they're no longer mirrors)
# TOPIC              STATE      SOURCE TOPIC       
# customer-orders    STOPPED    N/A    
# payment-events     STOPPED    N/A
# inventory-updates  STOPPED    N/A
```


### Step 3.4: Update Application Configurations

Update your application configs to point to DR cluster:

**Old configuration** (order-processing-service.properties):

```properties
bootstrap.servers=primary-kafka-1.us-east-1:9093,primary-kafka-2.us-east-1:9093
```

**New configuration**:

```properties
bootstrap.servers=dr-kafka-1.us-west-2:9093,dr-kafka-2.us-west-2:9093
```

Think of this like updating your DNS records or load balancer config to point to the backup datacenter.

### Step 3.5: Restart Applications on DR Infrastructure

Deploy and start your applications in us-west-2:

```bash
# Deploy order processing service to us-west-2
kubectl apply -f order-service-dr.yaml -n production

# Deploy payment processor to us-west-2
kubectl apply -f payment-service-dr.yaml -n production

# Deploy inventory service to us-west-2
kubectl apply -f inventory-service-dr.yaml -n production
```

**Your applications are now running on DR cluster**. Business continues!

### Step 3.6: Verify Everything Works

Test the full flow:

```bash
# Produce a test order
kafka-console-producer \
  --topic customer-orders \
  --bootstrap-server dr-kafka-1.us-west-2:9093 \
  --producer.config dr-admin.properties

> {"orderId": "TEST-001", "customerId": "12345", "amount": 99.99}

# Consume to verify
kafka-console-consumer \
  --topic customer-orders \
  --from-beginning \
  --bootstrap-server dr-kafka-1.us-west-2:9093 \
  --consumer.config dr-admin.properties \
  --max-messages 1
```

**Congratulations! You've successfully failed over**. Your business is operational in us-west-2.

## Phase 4: Primary Region Recovers (Failback - Manual Process)

**Monday, 10:00 AM** - AWS announces us-east-1 is back online. Your primary cluster is healthy again. Now you want to **fail back** to restore redundancy.

**Remember: With ZooKeeper, this is MANUAL**.

### Step 4.1: Verify Primary Cluster is Healthy

```bash
# Test connectivity to recovered primary
kafka-topics --list \
  --bootstrap-server primary-kafka-1.us-east-1:9093 \
  --command-config primary-admin.properties

# Should work now - primary is back!
```


### Step 4.2: Delete Old Cluster Link on Primary

The old link is pointing the wrong direction now:

```bash
# On PRIMARY cluster (us-east-1)
kafka-cluster-links --delete \
  --link primary-to-dr-link \
  --bootstrap-server primary-kafka-1.us-east-1:9093 \
  --command-config primary-admin.properties
```


### Step 4.3: Delete Old Topics on Primary

The data on primary is stale (from before the outage):

```bash
# WARNING: This deletes data!
kafka-topics --delete \
  --topic customer-orders \
  --bootstrap-server primary-kafka-1.us-east-1:9093 \
  --command-config primary-admin.properties

kafka-topics --delete \
  --topic payment-events \
  --bootstrap-server primary-kafka-1.us-east-1:9093 \
  --command-config primary-admin.properties

kafka-topics --delete \
  --topic inventory-updates \
  --bootstrap-server primary-kafka-1.us-east-1:9093 \
  --command-config primary-admin.properties
```


### Step 4.4: Create NEW Cluster Link (Opposite Direction)

Now create a link from DR → Primary (opposite direction):

```bash
# On PRIMARY cluster (us-east-1)
cat > new-link-config.properties <<EOF
bootstrap.servers=dr-kafka-1.us-west-2:9093,dr-kafka-2.us-west-2:9093
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="dr-password";
consumer.offset.sync.enable=true
consumer.offset.sync.ms=30000
EOF

kafka-cluster-links --create \
  --link dr-to-primary-link \
  --config-file new-link-config.properties \
  --bootstrap-server primary-kafka-1.us-east-1:9093 \
  --command-config primary-admin.properties
```


### Step 4.5: Create Mirror Topics on Primary (Role Reversal)

Primary now becomes the mirror, DR is the source:

```bash
# On PRIMARY cluster - create mirrors OF the DR cluster
kafka-mirrors --create \
  --mirror-topic customer-orders \
  --link dr-to-primary-link \
  --bootstrap-server primary-kafka-1.us-east-1:9093 \
  --command-config primary-admin.properties

kafka-mirrors --create \
  --mirror-topic payment-events \
  --link dr-to-primary-link \
  --bootstrap-server primary-kafka-1.us-east-1:9093 \
  --command-config primary-admin.properties

kafka-mirrors --create \
  --mirror-topic inventory-updates \
  --link dr-to-primary-link \
  --bootstrap-server primary-kafka-1.us-east-1:9093 \
  --command-config primary-admin.properties
```


### Step 4.6: Wait for Synchronization

Monitor lag until primary catches up:

```bash
# Check lag on primary cluster
kafka-mirrors --describe \
  --link dr-to-primary-link \
  --bootstrap-server primary-kafka-1.us-east-1:9093 \
  --command-config primary-admin.properties

# Wait until lag = 0 for all topics
# This could take minutes to hours depending on data volume
```


### Step 4.7: Stop Applications on DR

Once primary is caught up, stop applications in us-west-2:

```bash
# Stop all services in DR region
kubectl scale deployment order-service --replicas=0 -n production
kubectl scale deployment payment-service --replicas=0 -n production
kubectl scale deployment inventory-service --replicas=0 -n production
```


### Step 4.8: Failover on Primary (Make it Writable Again)

Now promote primary back to active:

```bash
# On PRIMARY cluster
kafka-mirrors --failover \
  customer-orders \
  payment-events \
  inventory-updates \
  --link dr-to-primary-link \
  --bootstrap-server primary-kafka-1.us-east-1:9093 \
  --command-config primary-admin.properties
```

**Primary is now writable again**!

### Step 4.9: Restart Applications on Primary

Update configs back to primary and restart:

```properties
bootstrap.servers=primary-kafka-1.us-east-1:9093,primary-kafka-2.us-east-1:9093
```

```bash
# Deploy back to us-east-1
kubectl scale deployment order-service --replicas=3 -n production
kubectl scale deployment payment-service --replicas=5 -n production
kubectl scale deployment inventory-service --replicas=2 -n production
```


### Step 4.10: Rebuild Original DR Setup

Now rebuild the original primary → DR link:

```bash
# Delete the reverse link on PRIMARY
kafka-cluster-links --delete \
  --link dr-to-primary-link \
  --bootstrap-server primary-kafka-1.us-east-1:9093 \
  --command-config primary-admin.properties

# Recreate original link on DR (repeat Phase 1 steps)
```

You're back to the original steady-state!

## The Pain Points You Just Experienced

Notice what you had to do manually with ZooKeeper:

1. **Delete and recreate** cluster links
2. **Delete old data** on the recovered cluster
3. **Wait for full resync** before failing back
4. **Multiple failover commands** in both directions
5. **Rebuild the original topology** from scratch

With **KRaft and bidirectional links**, all of this would be:

```bash
# Single command to fail back
confluent kafka mirror truncate-and-restore customer-orders payment-events inventory-updates \
  --link my-bidirectional-link

# Single command to restore original direction
confluent kafka mirror reverse-and-start customer-orders payment-events inventory-updates \
  --link my-bidirectional-link
```

That's it—two commands instead of 15+ steps!

## Key Takeaways

**Failover** (disaster happens):

- Works fine with ZooKeeper
- Single `kafka-mirrors --failover` command
- Fast and reliable

**Failback** (restoring redundancy):

- Painful with ZooKeeper
- Manual multi-step process
- Risk of mistakes during stress
- Downtime while resyncing

This is exactly why organizations migrate to KRaft—the operational complexity of DR is dramatically reduced.