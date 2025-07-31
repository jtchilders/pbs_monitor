# PBS Monitor Analytics Development Plan

## Overview

This document outlines the focused analytics features for PBS Monitor. The goal is to provide immediate, actionable insights about queue dynamics, job scoring patterns, backfill opportunities, and system usage trends.

## Vision Statement

Provide PBS Monitor users with essential HPC queue intelligence:
- **Queue Depth**: Understand total resource demand in the queue
- **Score Analysis**: Historical patterns of job scores that successfully transitioned to running
- **Backfill Opportunities**: Identify jobs that could run immediately without queueing
- **Usage Trends**: System utilization and submission patterns over time

## Target Features

### **1. Queue Depth Metrics**
- Total node-hours currently waiting in queue
- Integrated into existing `pbs-monitor status` command

### **2. Run Score Analysis** 
- Historical analysis of job scores at queue → run transition
- Binned by node count and walltime for pattern identification
- Command: `pbs-monitor analyze run-score`

### **3. Run-Now Analysis**
- Identify jobs that could run immediately on available resources
- Backfill opportunity identification for optimal job sizing
- Command: `pbs-monitor analyze run-now`

### **4. System Trends**
- Peak usage patterns, submission waves, completion rates
- Command: `pbs-monitor trends` with subcommands

## Implementation Phases

## Phase 1: Queue Depth Integration ✅ **COMPLETED**

### **Feature: Queue Depth in Status Command**
**Priority: HIGH** | **Complexity: LOW** | **Value: HIGH** | **Status: ✅ IMPLEMENTED**

#### Implementation:
```python
class QueueDepthCalculator:
    def calculate_total_node_hours(self, jobs: List[PBSJob]) -> float:
        """Calculate total node-hours waiting in queue"""
        total_node_hours = 0.0
        for job in jobs:
            if job.state.value == 'Q':  # Queued jobs only
                nodes = job.nodes or 1
                walltime_hours = self._parse_walltime_to_hours(job.walltime)
                total_node_hours += nodes * walltime_hours
        return total_node_hours
    
    def _parse_walltime_to_hours(self, walltime: str) -> float:
        """Convert walltime string to hours"""
        # Implementation for HH:MM:SS or DD:HH:MM:SS formats
```

#### CLI Integration:
```bash
pbs-monitor status                    # Include queue depth in default output
pbs-monitor status --queue-depth      # Detailed queue depth breakdown
```

#### Success Criteria:
- [x] Queue depth calculation integrated into status command
- [x] Accurate node-hours calculation for all queued jobs
- [x] Performance impact < 100ms additional time

#### ✅ **Implementation Status: COMPLETED**
**Commit:** `efd7638` | **Date:** Current | **Files:** 7 changed

**Key Achievements:**
- ✅ **QueueDepthCalculator class** created in `pbs_monitor/analytics/queue_depth.py`
- ✅ **CLI integration** with `--queue-depth` flag for detailed breakdown
- ✅ **System summary enhancement** with basic queue depth metrics
- ✅ **Comprehensive testing** verified correct QUEUED job filtering
- ✅ **Walltime parsing** supports HH:MM:SS and DD:HH:MM:SS formats
- ✅ **Performance validated** with <100ms overhead
- ✅ **Categorization** by node count (1-31, 32-127, etc.) and walltime ranges

**Usage:**
```bash
pbs-monitor status                    # Shows total node-hours waiting
pbs-monitor status --queue-depth      # Detailed breakdown by job categories
```

## Phase 2: Run Score Analysis (Week 2)

### **Feature: Historical Job Score Analysis**
**Priority: HIGH** | **Complexity: MEDIUM** | **Value: HIGH**

#### Implementation:
```python
class RunScoreAnalyzer:
    def __init__(self, database_manager: DatabaseManager):
        self.db = database_manager
        
        # Node count bins
        self.node_bins = [
            (1, 31, "1-31"),
            (32, 127, "32-127"), 
            (128, 255, "128-255"),
            (256, 1023, "256-1023"),
            (1024, float('inf'), "1024+")
        ]
        
        # Walltime bins (in hours)
        self.walltime_bins = [
            (0, 1, "0-60min"),
            (1, 3, "1-3hrs"),
            (3, 6, "3-6hrs"), 
            (6, 12, "6-12hrs"),
            (12, 18, "12-18hrs"),
            (18, 24, "18-24hrs"),
            (24, float('inf'), "24hrs+")
        ]
    
    def analyze_transition_scores(self, days: int = 30) -> pd.DataFrame:
        """Analyze job scores at queue->run transition"""
        
    def categorize_job(self, nodes: int, walltime_hours: float) -> Tuple[str, str]:
        """Categorize job into node and walltime bins"""
        
    def calculate_score_statistics(self, scores: List[float]) -> Dict:
        """Calculate mean, std dev, count for score list"""
```

#### CLI Integration:
```bash
pbs-monitor analyze run-score                    # Default 30-day analysis
pbs-monitor analyze run-score --days 60         # Specify time window
pbs-monitor analyze run-score --format table    # Table format (default)
pbs-monitor analyze run-score --format csv      # CSV export
```

#### Output Format:
```
Job Score Analysis: Queue → Run Transition (Last 30 days)

Node Count    | 0-60min      | 1-3hrs       | 3-6hrs       | 6-12hrs      | 12-18hrs     | 18-24hrs
1-31          | 1250 ± 150   | 1180 ± 200   | 1100 ± 180   | 1050 ± 220   | 980 ± 190    | 920 ± 250
32-127        | 1400 ± 180   | 1320 ± 210   | 1250 ± 195   | 1180 ± 240   | 1120 ± 220   | 1050 ± 280
128-255       | 1550 ± 220   | 1480 ± 250   | 1400 ± 230   | 1320 ± 280   | 1250 ± 260   | 1180 ± 320
256-1023      | 1700 ± 280   | 1620 ± 310   | 1550 ± 290   | 1480 ± 340   | 1400 ± 320   | 1320 ± 380
1024+         | 1850 ± 350   | 1780 ± 380   | 1700 ± 360   | 1620 ± 420   | 1550 ± 400   | 1480 ± 450

Note: Values show Average Score ± Standard Deviation. Sample sizes vary by bin.
```

#### Success Criteria:
- [ ] Historical job score analysis from database
- [ ] Proper binning by node count and walltime
- [ ] Statistical calculations (mean, std dev) for each bin
- [ ] Table output with clear formatting

## Phase 3: Run-Now Analysis (Week 3)

### **Feature: Immediate Backfill Opportunities**
**Priority: HIGH** | **Complexity: MEDIUM** | **Value: HIGH**

#### Implementation:
```python
class RunNowAnalyzer:
    def __init__(self, pbs_commands: PBSCommands):
        self.pbs = pbs_commands
        
    def analyze_immediate_opportunities(self) -> Dict[str, Any]:
        """Find jobs that could run immediately without queueing"""
        
        # Get current system state
        nodes = self.pbs.pbsnodes()
        queued_jobs = self.pbs.qstat_jobs()
        
        available_nodes = self._count_available_nodes(nodes)
        
        # Find opportunities
        largest_job = self._find_largest_immediate_job(available_nodes)
        longest_job = self._find_longest_immediate_job(available_nodes)
        
        return {
            'available_nodes': available_nodes,
            'largest_immediate': largest_job,
            'longest_immediate': longest_job,
            'analysis_time': datetime.now()
        }
    
    def _count_available_nodes(self, nodes: List[PBSNode]) -> int:
        """Count nodes available for immediate scheduling"""
        
    def _find_largest_immediate_job(self, available_nodes: int) -> Dict:
        """Find largest job (by nodes) that could run immediately"""
        
    def _find_longest_immediate_job(self, available_nodes: int) -> Dict:
        """Find longest job that could run on available nodes"""
```

#### CLI Integration:
```bash
pbs-monitor analyze run-now                     # Show immediate opportunities
pbs-monitor analyze run-now --min-walltime 10m  # Minimum walltime filter
```

#### Output Format:
```
Immediate Run Opportunities (No Queueing Required)

Available Nodes: 245 nodes ready for immediate scheduling

Largest Job Opportunity:
   Recommended: 240 nodes, 10+ minute walltime
   Rationale: Maximum nodes available with small buffer for scheduling overhead
   Expected: Job would start within 5 minutes of submission

Longest Duration Opportunity:  
   Recommended: 32 nodes, 12+ hour walltime
   Rationale: Based on historical patterns, 32-node jobs complete successfully
   Expected: Job would start immediately and have minimal competition

Current Competition:
   Jobs waiting for 200+ nodes: 3 jobs
   Jobs waiting for 32-127 nodes: 12 jobs
   Jobs waiting for 1-31 nodes: 45 jobs

Recommendation: Submit 240-node short job OR 32-node long job for best immediate scheduling.
```

#### Success Criteria:
- [ ] Real-time analysis of available nodes
- [ ] Identification of largest immediate opportunity
- [ ] Identification of longest duration opportunity
- [ ] Clear recommendations with rationale

## Phase 4: System Trends Analysis (Week 4)

### **Feature: Usage Pattern Trends**
**Priority: MEDIUM** | **Complexity: MEDIUM** | **Value: HIGH**

#### Implementation:
```python
class TrendsAnalyzer:
    def __init__(self, database_manager: DatabaseManager):
        self.db = database_manager
    
    def analyze_usage_patterns(self, days: int = 30) -> Dict:
        """Analyze peak usage times and patterns"""
        
    def analyze_submission_patterns(self, days: int = 30) -> Dict:
        """Analyze job submission waves and timing"""
        
    def analyze_completion_rates(self, days: int = 30) -> Dict:
        """Analyze job completion vs failure rates"""
```

#### CLI Integration:
```bash
pbs-monitor trends                              # Interactive menu
pbs-monitor trends usage-patterns               # Peak usage analysis
pbs-monitor trends submissions                  # Submission wave analysis  
pbs-monitor trends completion                   # Completion rate analysis
pbs-monitor trends --days 60                   # Specify analysis window
```

#### Success Criteria:
- [ ] Peak usage time identification
- [ ] Submission pattern analysis
- [ ] Completion rate trends
- [ ] Clear trend visualization in terminal

## Technical Implementation Details

### **Database Integration**
- Leverage existing Phase 2 database infrastructure
- Use job history tables for score and trends analysis
- Efficient queries for large historical datasets

### **Performance Requirements**
- All analytics complete within 30 seconds
- Minimal impact on existing CLI commands
- Efficient data aggregation and caching

### **Data Sources**
- **Real-time**: Current job queue, node status
- **Historical**: Job completion records, score transitions
- **Trends**: Time-series job submission and completion data

## CLI Command Structure

### **Enhanced Existing Commands**
```bash
# Status command (enhanced)
pbs-monitor status                              # Now includes queue depth
pbs-monitor status --queue-depth                # Detailed queue metrics
```

### **New Analytics Commands**
```bash
# Analysis commands
pbs-monitor analyze run-score                   # Score transition analysis
pbs-monitor analyze run-score --days 60        # Specify time window
pbs-monitor analyze run-now                     # Immediate opportunities
pbs-monitor analyze run-now --min-walltime 30m # Filter opportunities

# Trends commands  
pbs-monitor trends                              # Interactive trends menu
pbs-monitor trends usage-patterns               # Peak usage analysis
pbs-monitor trends submissions                  # Submission patterns
pbs-monitor trends completion                   # Completion rates
pbs-monitor trends --days 90                   # Specify analysis period
```

## Success Metrics

### **Feature Adoption**
- [ ] Queue depth metrics used in daily status checks
- [ ] Run-score analysis helps users understand score requirements
- [ ] Run-now analysis leads to better job submission timing
- [ ] Trends analysis provides insights into system patterns

### **Technical Performance**
- [ ] Queue depth calculation: < 100ms overhead
- [ ] Run-score analysis: < 30 seconds for 30-day window  
- [ ] Run-now analysis: < 15 seconds for current state
- [ ] Trends analysis: < 45 seconds for 30-day window

### **User Value**
- [ ] Users understand queue competition via queue depth
- [ ] Users optimize job scores based on historical patterns
- [ ] Users time submissions based on backfill opportunities
- [ ] Users understand system usage patterns for planning

## Implementation Timeline

### **Week 1: Queue Depth Integration** ✅ **COMPLETED**
- ✅ Implement queue depth calculation logic
- ✅ Integrate into existing status command
- ✅ Add detailed queue depth option
- ✅ Testing and validation

### **Week 2: Run Score Analysis**
- Database queries for historical job scores
- Binning logic for nodes and walltime
- Statistical calculations and table formatting
- CLI command implementation

### **Week 3: Run-Now Analysis**
- Real-time node availability analysis
- Backfill opportunity identification
- Recommendation engine logic
- Output formatting and CLI integration

### **Week 4: System Trends**
- Historical data analysis for usage patterns
- Submission wave identification
- Completion rate calculations
- Trends visualization in terminal

## Dependencies & Prerequisites

### **Technical Dependencies**
- [ ] **Phase 2 Database**: Historical job data required
- [ ] **Real-time data**: Current job and node status
- [ ] **Python packages**: pandas, numpy for data analysis

### **Data Requirements**
- [ ] **Job score history**: Scores at state transition points
- [ ] **Job lifecycle data**: Submission, start, completion times
- [ ] **Node status data**: Real-time availability information

## Risk Assessment & Mitigation

### **Technical Risks**
| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Performance on large datasets | Medium | Low | Efficient queries, data pagination |
| Database query complexity | Medium | Medium | Query optimization, indexing |
| Real-time data accuracy | High | Low | Data validation, error handling |

### **Implementation Risks**  
| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Feature complexity creep | Medium | Medium | Stick to defined scope, resist additions |
| Integration complexity | Medium | Low | Leverage existing CLI framework |
| User adoption | Low | Low | Clear documentation, examples |

## Future Enhancements

### **Beyond Core Features**
- **Predictive modeling**: ML-based wait time prediction
- **Advanced scoring**: Custom score formulas and analysis
- **Notification system**: Alerts for optimal submission times
- **Historical comparisons**: Year-over-year trend analysis

## Conclusion

This focused analytics implementation provides immediate value through four core features: queue depth visibility, historical score analysis, backfill opportunities, and system trends. The 4-week implementation timeline ensures rapid delivery of practical insights while maintaining the existing PBS Monitor architecture and performance standards.

Each feature addresses specific user needs: understanding queue competition, optimizing job parameters based on historical success patterns, identifying immediate run opportunities, and recognizing system usage trends for better planning. 