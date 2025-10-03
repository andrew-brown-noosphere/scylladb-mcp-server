"""Advanced Cost Calculator matching ScyllaDB pricing calculator logic."""

import math
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass


@dataclass
class WorkloadProfile:
    """Represents hourly workload patterns."""
    baseline_reads: int
    baseline_writes: int
    peak_reads: int
    peak_writes: int
    peak_read_hours: List[int]  # Hours when peak reads occur (0-23)
    peak_write_hours: List[int]  # Hours when peak writes occur (0-23)


@dataclass
class CostParameters:
    """Cost calculation parameters."""
    storage_gb: int
    item_size_bytes: int
    storage_compression: float = 0.5  # 50% compression default
    network_compression: float = 0.5  # 50% compression default
    replication_factor: int = 3
    storage_utilization: float = 0.9  # 90% utilization
    overprovisioning: float = 0.2  # 20% overhead
    

class ScyllaDBCostCalculator:
    """Advanced cost calculator matching ScyllaDB's online calculator."""
    
    # DynamoDB Pricing (us-east-1)
    DYNAMODB_READ_UNIT_COST = 0.25 / 1_000_000  # Per read
    DYNAMODB_WRITE_UNIT_COST = 1.25 / 1_000_000  # Per write
    DYNAMODB_STORAGE_COST_GB = 0.25  # Per GB per month
    
    # ScyllaDB Instance Types and Performance
    INSTANCE_TYPES = {
        'i3en.large': {
            'vcpus': 2,
            'memory_gb': 16,
            'storage_gb': 1250,
            'network_gbps': 3.125,
            'cost_per_hour': 0.156,
            'ops_per_sec': 25000
        },
        'i3en.xlarge': {
            'vcpus': 4,
            'memory_gb': 32,
            'storage_gb': 2500,
            'network_gbps': 3.125,
            'cost_per_hour': 0.312,
            'ops_per_sec': 50000
        },
        'i3en.2xlarge': {
            'vcpus': 8,
            'memory_gb': 64,
            'storage_gb': 5000,
            'network_gbps': 3.125,
            'cost_per_hour': 0.624,
            'ops_per_sec': 100000
        },
        'i3en.3xlarge': {
            'vcpus': 12,
            'memory_gb': 96,
            'storage_gb': 7500,
            'network_gbps': 3.875,
            'cost_per_hour': 0.936,
            'ops_per_sec': 150000
        },
        'i3en.6xlarge': {
            'vcpus': 24,
            'memory_gb': 192,
            'storage_gb': 15000,
            'network_gbps': 7.5,
            'cost_per_hour': 1.872,
            'ops_per_sec': 300000
        },
        'i3en.12xlarge': {
            'vcpus': 48,
            'memory_gb': 384,
            'storage_gb': 30000,
            'network_gbps': 15,
            'cost_per_hour': 3.744,
            'ops_per_sec': 600000
        }
    }
    
    def __init__(self):
        self.hours_per_month = 730  # Average hours in a month
        
    def calculate_dynamodb_cost(self, workload: WorkloadProfile, params: CostParameters) -> Dict[str, float]:
        """Calculate DynamoDB costs with on-demand pricing."""
        
        # Calculate total monthly operations
        total_read_hours = len(workload.peak_read_hours)
        total_write_hours = len(workload.peak_write_hours)
        
        baseline_read_hours = 24 - total_read_hours
        baseline_write_hours = 24 - total_write_hours
        
        # Monthly reads
        monthly_reads = (
            workload.baseline_reads * baseline_read_hours * 30.5 * 3600 +
            workload.peak_reads * total_read_hours * 30.5 * 3600
        )
        
        # Monthly writes
        monthly_writes = (
            workload.baseline_writes * baseline_write_hours * 30.5 * 3600 +
            workload.peak_writes * total_write_hours * 30.5 * 3600
        )
        
        # Calculate costs
        read_cost = monthly_reads * self.DYNAMODB_READ_UNIT_COST
        write_cost = monthly_writes * self.DYNAMODB_WRITE_UNIT_COST
        storage_cost = params.storage_gb * self.DYNAMODB_STORAGE_COST_GB
        
        return {
            'read_cost': read_cost,
            'write_cost': write_cost,
            'storage_cost': storage_cost,
            'total': read_cost + write_cost + storage_cost,
            'monthly_reads': monthly_reads,
            'monthly_writes': monthly_writes
        }
    
    def calculate_scylladb_cost(self, workload: WorkloadProfile, params: CostParameters) -> Dict[str, float]:
        """Calculate ScyllaDB costs with advanced logic."""
        
        # Calculate peak requirements
        peak_ops = max(workload.peak_reads + workload.peak_writes,
                      workload.baseline_reads + workload.baseline_writes)
        
        # Add overprovisioning
        required_ops = peak_ops * (1 + params.overprovisioning)
        
        # Calculate storage requirements with compression and replication
        effective_storage = (params.storage_gb * params.replication_factor * 
                           (1 - params.storage_compression)) / params.storage_utilization
        
        # Select optimal instance type
        instance_type, instance_count = self._select_instances(required_ops, effective_storage)
        instance_info = self.INSTANCE_TYPES[instance_type]
        
        # Calculate compute cost
        compute_cost = instance_count * instance_info['cost_per_hour'] * self.hours_per_month
        
        # Check if additional storage needed
        total_instance_storage = instance_count * instance_info['storage_gb']
        additional_storage_needed = max(0, effective_storage - total_instance_storage)
        
        # EBS storage cost if needed
        ebs_cost = additional_storage_needed * 0.10  # $0.10 per GB for gp3
        
        # Network transfer costs (simplified - assumes cross-AZ traffic)
        avg_ops_per_sec = (workload.baseline_reads + workload.baseline_writes + 
                          (workload.peak_reads + workload.peak_writes) * len(workload.peak_read_hours) / 24)
        
        # Estimate network transfer (GB/month)
        network_gb = (avg_ops_per_sec * params.item_size_bytes * 
                     (1 - params.network_compression) * self.hours_per_month * 3600 / 1e9)
        
        # Cross-AZ transfer cost ($0.02/GB)
        network_cost = network_gb * 0.66 * 0.02  # 2/3 traffic crosses AZ in 3-AZ deployment
        
        return {
            'compute_cost': compute_cost,
            'storage_cost': ebs_cost,
            'network_cost': network_cost,
            'total': compute_cost + ebs_cost + network_cost,
            'instance_type': instance_type,
            'instance_count': instance_count,
            'total_ops_capacity': instance_count * instance_info['ops_per_sec']
        }
    
    def _select_instances(self, required_ops: float, required_storage: float) -> Tuple[str, int]:
        """Select optimal instance type and count."""
        
        best_config = None
        best_cost = float('inf')
        
        for instance_type, info in self.INSTANCE_TYPES.items():
            # Calculate instances needed for ops
            instances_for_ops = math.ceil(required_ops / info['ops_per_sec'])
            
            # Calculate instances needed for storage
            instances_for_storage = math.ceil(required_storage / info['storage_gb'])
            
            # Take the maximum
            instance_count = max(instances_for_ops, instances_for_storage, 3)  # Min 3 for HA
            
            # Calculate monthly cost
            monthly_cost = instance_count * info['cost_per_hour'] * self.hours_per_month
            
            # Check if we need EBS
            total_storage = instance_count * info['storage_gb']
            if required_storage > total_storage:
                ebs_needed = required_storage - total_storage
                monthly_cost += ebs_needed * 0.10
            
            if monthly_cost < best_cost:
                best_cost = monthly_cost
                best_config = (instance_type, instance_count)
        
        return best_config
    
    def generate_calculator_url(self, workload: WorkloadProfile, params: CostParameters) -> str:
        """Generate URL for ScyllaDB online calculator with parameters."""
        
        base_url = "https://calculator.scylladb.com/"
        
        # Build series data (24 hour pattern)
        read_series = []
        write_series = []
        
        for hour in range(24):
            if hour in workload.peak_read_hours:
                read_series.append(str(workload.peak_reads))
            else:
                read_series.append(str(workload.baseline_reads))
                
            if hour in workload.peak_write_hours:
                write_series.append(str(workload.peak_writes))
            else:
                write_series.append(str(workload.baseline_writes))
        
        # Calculate totals
        total_reads = sum(int(r) for r in read_series) * 3600 * 30.5
        total_writes = sum(int(w) for w in write_series) * 3600 * 30.5
        
        # Build URL parameters
        params_dict = {
            'pricing': 'demand',
            'storageGB': params.storage_gb,
            'itemSizeB': params.item_size_bytes,
            'tableClass': 'standard',
            'baselineReads': workload.baseline_reads,
            'baselineWrites': workload.baseline_writes,
            'peakReads': workload.peak_reads,
            'peakWrites': workload.peak_writes,
            'peakDurationReads': len(workload.peak_read_hours),
            'peakDurationWrites': len(workload.peak_write_hours),
            'reserved': 0,
            'readConst': 100,
            'service': 'dynamodb',
            'totalReads': total_reads,
            'totalWrites': total_writes,
            'reservedReads': 0,
            'reservedWrites': 0,
            'overprovisioned': int(params.overprovisioning * 100),
            'seriesReads': '.'.join(read_series),
            'seriesWrites': '.'.join(write_series),
            'workload': 'baselinePeak',
            'replication': params.replication_factor,
            'storageCompression': int(params.storage_compression * 100),
            'storageUtilization': int(params.storage_utilization * 100),
            'networkCompression': int(params.network_compression * 100),
            'provider': 'aws',
            'discounts': '0%2C0.12%2C0.28'
        }
        
        # Build query string
        query_string = '&'.join(f"{k}={v}" for k, v in params_dict.items())
        
        return f"{base_url}?{query_string}"


def calculate_advanced_cost(reads_per_sec: int, writes_per_sec: int, storage_gb: int,
                          item_size_kb: float = 1, pattern: str = 'steady') -> str:
    """Calculate costs using advanced logic matching ScyllaDB calculator."""
    
    calculator = ScyllaDBCostCalculator()
    
    # Create workload profile based on pattern
    if pattern == 'bursty':
        # 4 hours of peak traffic
        workload = WorkloadProfile(
            baseline_reads=reads_per_sec,
            baseline_writes=writes_per_sec,
            peak_reads=int(reads_per_sec * 2.5),  # 2.5x peak
            peak_writes=int(writes_per_sec * 2.5),
            peak_read_hours=[11, 12, 13, 14],  # Lunch hours
            peak_write_hours=[9, 10, 17, 18]   # Business hours
        )
    elif pattern == 'time_series':
        # Constant high writes, periodic reads
        workload = WorkloadProfile(
            baseline_reads=int(reads_per_sec * 0.2),
            baseline_writes=writes_per_sec,
            peak_reads=reads_per_sec,
            peak_writes=writes_per_sec,
            peak_read_hours=[8, 9, 10, 11, 12, 13, 14, 15, 16, 17],  # Business hours
            peak_write_hours=[]  # Constant writes
        )
    else:  # steady
        workload = WorkloadProfile(
            baseline_reads=reads_per_sec,
            baseline_writes=writes_per_sec,
            peak_reads=reads_per_sec,
            peak_writes=writes_per_sec,
            peak_read_hours=[],
            peak_write_hours=[]
        )
    
    # Set parameters
    params = CostParameters(
        storage_gb=storage_gb,
        item_size_bytes=int(item_size_kb * 1024),
        storage_compression=0.5,
        network_compression=0.5,
        replication_factor=3,
        storage_utilization=0.9,
        overprovisioning=0.2
    )
    
    # Calculate costs
    dynamodb_costs = calculator.calculate_dynamodb_cost(workload, params)
    scylladb_costs = calculator.calculate_scylladb_cost(workload, params)
    
    # Calculate savings
    savings_percent = ((dynamodb_costs['total'] - scylladb_costs['total']) / 
                      dynamodb_costs['total'] * 100) if dynamodb_costs['total'] > 0 else 0
    
    # Generate calculator URL
    calculator_url = calculator.generate_calculator_url(workload, params)
    
    # Format response
    response = f"""💰 Advanced Cost Analysis (Pattern: {pattern})

📊 Workload Profile:
- Baseline: {workload.baseline_reads:,} reads/sec, {workload.baseline_writes:,} writes/sec
- Peak: {workload.peak_reads:,} reads/sec, {workload.peak_writes:,} writes/sec
- Storage: {storage_gb:,}GB ({params.storage_compression*100:.0f}% compression)
- Item Size: {item_size_kb}KB

💵 DynamoDB Monthly Costs:
- Reads: ${dynamodb_costs['read_cost']:,.2f} ({dynamodb_costs['monthly_reads']/1e9:.1f}B reads)
- Writes: ${dynamodb_costs['write_cost']:,.2f} ({dynamodb_costs['monthly_writes']/1e9:.1f}B writes)
- Storage: ${dynamodb_costs['storage_cost']:,.2f}
- TOTAL: ${dynamodb_costs['total']:,.2f}/month

🚀 ScyllaDB Monthly Costs:
- Compute: {scylladb_costs['instance_count']}x {scylladb_costs['instance_type']} = ${scylladb_costs['compute_cost']:,.2f}
- Storage: ${scylladb_costs['storage_cost']:,.2f}
- Network: ${scylladb_costs['network_cost']:,.2f}
- TOTAL: ${scylladb_costs['total']:,.2f}/month
- Capacity: {scylladb_costs['total_ops_capacity']:,} ops/sec

📈 Cost Reduction: {savings_percent:.0f}%

🔗 Verify with ScyllaDB Calculator:
{calculator_url}

Note: Calculations include compression, replication, and overprovisioning to match production deployments."""
    
    return response