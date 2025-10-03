"""User-friendly workload templates for different application types.

ScyllaDB is architected for massive scale - these profiles reflect
real production workloads from companies like Discord, Comcast, and
Palo Alto Networks.
"""

from typing import Dict, Tuple
from dataclasses import dataclass


@dataclass
class ApplicationProfile:
    """Represents a typical application workload profile."""
    name: str
    description: str
    baseline_reads: int
    baseline_writes: int
    peak_reads: int
    peak_writes: int
    storage_gb: int
    item_size_kb: float
    peak_hours: list  # Hours when peak occurs
    example_companies: list
    scylladb_sweet_spot: bool  # True if this is ideal for ScyllaDB
    

# Real-world application profiles based on industry patterns
APPLICATION_PROFILES = {
    # ========== WHERE SCYLLADB DOMINATES ==========
    
    "social_messaging_platform": ApplicationProfile(
        name="Social Messaging Platform (Discord-scale)",
        description="Billions of messages, millions of concurrent users",
        baseline_reads=50000,
        baseline_writes=20000,
        peak_reads=200000,
        peak_writes=80000,
        storage_gb=50000,  # 50TB
        item_size_kb=1,
        peak_hours=[16, 17, 18, 19, 20, 21, 22, 23],  # Evening gaming/social
        example_companies=["Discord", "Gaming chat", "Social platforms"],
        scylladb_sweet_spot=True
    ),
    
    "adtech_platform": ApplicationProfile(
        name="AdTech Real-time Bidding",
        description="Sub-10ms response required, billions of requests/day",
        baseline_reads=100000,
        baseline_writes=50000,
        peak_reads=500000,
        peak_writes=250000,
        storage_gb=10000,
        item_size_kb=0.5,
        peak_hours=list(range(6, 22)),  # All day
        example_companies=["DSP platforms", "Ad exchanges", "RTB systems"],
        scylladb_sweet_spot=True
    ),
    
    "cybersecurity_platform": ApplicationProfile(
        name="Cybersecurity Analytics (Palo Alto Networks-scale)",
        description="Threat detection, billions of events analyzed",
        baseline_reads=20000,
        baseline_writes=100000,  # Write-heavy
        peak_reads=100000,
        peak_writes=500000,
        storage_gb=100000,  # 100TB
        item_size_kb=2,
        peak_hours=list(range(24)),  # 24/7 threats
        example_companies=["Palo Alto Networks", "CrowdStrike", "SentinelOne"],
        scylladb_sweet_spot=True
    ),
    
    "iot_telemetry": ApplicationProfile(
        name="Massive IoT Platform (Comcast-scale)",
        description="Millions of devices, continuous telemetry",
        baseline_reads=10000,
        baseline_writes=200000,
        peak_reads=50000,
        peak_writes=1000000,
        storage_gb=500000,  # 500TB
        item_size_kb=0.1,
        peak_hours=list(range(6, 23)),  # Daytime activity
        example_companies=["Comcast Xfinity", "Smart city platforms", "Industrial IoT"],
        scylladb_sweet_spot=True
    ),
    
    "streaming_analytics": ApplicationProfile(
        name="Streaming Analytics Platform",
        description="Real-time analytics on streaming data",
        baseline_reads=30000,
        baseline_writes=30000,
        peak_reads=150000,
        peak_writes=150000,
        storage_gb=20000,
        item_size_kb=2,
        peak_hours=[19, 20, 21, 22],  # Prime time
        example_companies=["Disney+ Hotstar", "Live sports streaming", "Twitch analytics"],
        scylladb_sweet_spot=True
    ),
    
    # ========== GROWING INTO SCYLLADB ==========
    
    "fintech_scale": ApplicationProfile(
        name="FinTech at Scale",
        description="Payment processing, fraud detection, real-time",
        baseline_reads=5000,
        baseline_writes=2000,
        peak_reads=50000,
        peak_writes=20000,
        storage_gb=5000,
        item_size_kb=1,
        peak_hours=[9, 10, 11, 14, 15, 16],  # Trading hours
        example_companies=["Robinhood-scale", "Crypto exchanges", "Payment processors"],
        scylladb_sweet_spot=True
    ),
    
    "gaming_backend_large": ApplicationProfile(
        name="Large Gaming Backend",
        description="MMO or battle royale game backend",
        baseline_reads=10000,
        baseline_writes=5000,
        peak_reads=100000,
        peak_writes=50000,
        storage_gb=10000,
        item_size_kb=1,
        peak_hours=[18, 19, 20, 21, 22, 23],  # Evening gaming
        example_companies=["Epic Games scale", "MMO games", "Battle royale"],
        scylladb_sweet_spot=True
    ),
    
    # ========== REALISTIC BUT SMALLER SCALE ==========
    
    "ecommerce_growing": ApplicationProfile(
        name="Growing E-commerce Platform",
        description="$100M+ revenue, considering DynamoDB alternatives",
        baseline_reads=5000,
        baseline_writes=1500,
        peak_reads=25000,
        peak_writes=7500,
        storage_gb=2000,
        item_size_kb=5,
        peak_hours=[11, 12, 13, 14, 18, 19, 20, 21, 22],
        example_companies=["Mid-market retailer", "Flash sale sites", "B2B marketplace"],
        scylladb_sweet_spot=True  # Cost savings justify migration
    ),
    
    "saas_platform": ApplicationProfile(
        name="Enterprise SaaS Platform",
        description="Multi-tenant B2B with real-time analytics",
        baseline_reads=3000,
        baseline_writes=1000,
        peak_reads=30000,
        peak_writes=10000,
        storage_gb=5000,
        item_size_kb=10,
        peak_hours=[9, 10, 11, 13, 14, 15, 16],
        example_companies=["Analytics platforms", "Business intelligence", "CRM at scale"],
        scylladb_sweet_spot=True
    ),
    
    # ========== TOO SMALL FOR SCYLLADB ==========
    
    "startup_small": ApplicationProfile(
        name="Small Startup",
        description="Better off with DynamoDB on-demand or RDS",
        baseline_reads=100,
        baseline_writes=50,
        peak_reads=500,
        peak_writes=250,
        storage_gb=100,
        item_size_kb=2,
        peak_hours=[9, 10, 11, 12, 13, 14, 15, 16, 17],
        example_companies=["MVP", "Small SaaS", "Local apps"],
        scylladb_sweet_spot=False
    )
}


def get_scylladb_recommendations() -> Dict[str, ApplicationProfile]:
    """Get workload profiles where ScyllaDB excels."""
    return {k: v for k, v in APPLICATION_PROFILES.items() if v.scylladb_sweet_spot}


def calculate_savings_for_profile(profile_id: str) -> Dict[str, float]:
    """Calculate potential savings for a specific profile."""
    from advanced_cost_calculator import ScyllaDBCostCalculator, WorkloadProfile, CostParameters
    
    profile = APPLICATION_PROFILES.get(profile_id)
    if not profile:
        return {}
    
    # Create workload profile
    workload = WorkloadProfile(
        baseline_reads=profile.baseline_reads,
        baseline_writes=profile.baseline_writes,
        peak_reads=profile.peak_reads,
        peak_writes=profile.peak_writes,
        peak_read_hours=profile.peak_hours,
        peak_write_hours=profile.peak_hours
    )
    
    # Set parameters
    params = CostParameters(
        storage_gb=profile.storage_gb,
        item_size_bytes=int(profile.item_size_kb * 1024)
    )
    
    # Calculate costs
    calculator = ScyllaDBCostCalculator()
    dynamodb_costs = calculator.calculate_dynamodb_cost(workload, params)
    scylladb_costs = calculator.calculate_scylladb_cost(workload, params)
    
    return {
        'dynamodb_monthly': dynamodb_costs['total'],
        'scylladb_monthly': scylladb_costs['total'],
        'savings_monthly': dynamodb_costs['total'] - scylladb_costs['total'],
        'savings_percent': ((dynamodb_costs['total'] - scylladb_costs['total']) / 
                           dynamodb_costs['total'] * 100) if dynamodb_costs['total'] > 0 else 0,
        'scylladb_config': f"{scylladb_costs['instance_count']}x {scylladb_costs['instance_type']}"
    }


def format_profile_with_savings(profile_id: str) -> str:
    """Format profile with calculated savings."""
    profile = APPLICATION_PROFILES.get(profile_id)
    if not profile:
        return "Unknown profile"
    
    savings = calculate_savings_for_profile(profile_id)
    
    return f"""
📱 {profile.name}
{profile.description}

Real-world examples:
{', '.join(profile.example_companies)}

📊 Workload Characteristics:
- Baseline: {profile.baseline_reads:,} reads/sec, {profile.baseline_writes:,} writes/sec  
- Peak: {profile.peak_reads:,} reads/sec, {profile.peak_writes:,} writes/sec
- Storage: {profile.storage_gb:,}GB
- Peak hours: {len(profile.peak_hours)} hours/day

💰 Cost Analysis:
- DynamoDB: ${savings['dynamodb_monthly']:,.2f}/month
- ScyllaDB: ${savings['scylladb_monthly']:,.2f}/month ({savings['scylladb_config']})
- Monthly savings: ${savings['savings_monthly']:,.2f} ({savings['savings_percent']:.0f}% reduction)
- Annual savings: ${savings['savings_monthly'] * 12:,.2f}

{"✅ IDEAL for ScyllaDB - Designed for this scale!" if profile.scylladb_sweet_spot else "⚠️  May be too small for ScyllaDB - consider managed services"}
"""