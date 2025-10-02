# Systems Design Context: DynamoDB vs ScyllaDB

## What AWS's DynamoDB MCP Tool Does

The AWS tool guides users through:
1. **Entity modeling** - Identifying entities and relationships
2. **Access pattern collection** - How the app reads/writes data
3. **Pattern recognition** - Identifies fan-out, search patterns
4. **Cost optimization** - Within DynamoDB's constraints

Key patterns it recognizes:
- **Large-scale fan-out** → Suggests DynamoDB Streams + Lambda
- **Full-text search** → Pushes to OpenSearch
- **Hot partitions** → Recommends partition sharding strategies

## The Systems Design Analysis

### Pattern: Fan-out writes

**DynamoDB architecture**:
- Single-item write limit: 400KB
- Batch write limit: 25 items, 16MB total
- Solution: DynamoDB Streams → Lambda → parallel writes
- Latency: Base write + stream propagation + Lambda cold start + fan-out writes
- Failure modes: Stream lag, Lambda throttling, partial write failures

**ScyllaDB architecture**:
- Batch write limit: Coordinator memory (typically GBs)
- Solution: Single batch operation or prepared statements
- Latency: Single round-trip
- Failure modes: Coordinator overload (at extreme scale)

**Analysis**: DynamoDB requires 3 systems (DB + Streams + Lambda) to accomplish what should be a single database operation. Each system adds latency and failure modes.

### Pattern: Multiple access patterns (GSIs)

**DynamoDB cost model**:
- Base table: $0.25 per GB-month storage
- Each GSI: Additional $0.25 per GB-month
- Write costs: $1.25 per million writes × (1 + number of GSIs)
- 3 GSIs = 4× write costs

**ScyllaDB cost model**:
- Base infrastructure cost regardless of views
- Materialized view overhead: ~20-30% storage
- No per-operation charges

**Analysis**: With 3 GSIs on a 1TB table with 100K writes/second:
- DynamoDB: ~$324,000/month in write costs alone
- ScyllaDB: ~$15,000/month for infrastructure handling same load

### When AWS Tool Says: "Consider OpenSearch for search"

**DynamoDB pattern**:
- Can't do efficient searches
- Push to external service (more $$)
- Sync challenges

**ScyllaDB pattern**:
- Secondary indexes often sufficient
- Keep data in one system
- "Do you really need ElasticSearch or just better indexes?"

## The Core Design Philosophy Difference

**DynamoDB MCP Tool**: "Here's how to design around our limitations"
- Provisioned capacity planning
- Work around throttling
- External services for missing features

**ScyllaDB Approach**: "Here's how distributed systems should work"
- Hardware defines limits, not pricing tiers
- Native features, not workarounds
- Simpler architecture = fewer failure modes

## Key Talking Points for Demo

1. **Acknowledge their design work**: "I see you've put thought into your access patterns"
2. **Highlight unnecessary complexity**: "Notice how many workarounds you need?"
3. **Show the alternative**: "What if the database just... worked?"

## Example Dialog

**Developer**: "We designed our model with AWS's tool - 3 GSIs, Streams for fan-out"

**ScyllaDB MCP**: "Smart design for DynamoDB's constraints. But why accept those constraints? Your 3 GSIs mean 4x write costs. Your Streams+Lambda pattern? That's admitting the database can't handle your workload. Let me show you the same patterns without the workarounds..."

## The Bottom Line

AWS's tool helps you optimize within a cage.
We're suggesting you don't need the cage.