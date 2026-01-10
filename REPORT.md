# Google Cluster Trace Analysis Report

**Team Members:** Edoardo Cecchini, Giuseppe Di Stefano

---

## 1. Introduction

Brief description of the Google Cluster Trace dataset and analysis scope.

**Tools Used:**
- Apache Spark with PySpark
- Python for data analysis with pyspark.sql (SparkSession, functions) and pyspark.sql.window (Window)

## 2. Analysis Results

### Q1: Machine CPU Distribution

**Question:** What is the distribution of machines according to their CPU capacity?

#### Implementation and Challenges:
This analysis was straightforward with no significant challenges. We grouped machines by their CPU capacity and counted the occurrences of each value.

#### Results:

| CPU Capacity | Machine Count |
|--------------|---------------|
| 0.25         | 510           |
| 0.5          | 35015         |
| 1.0          | 2223          |
| NULL         | 32            |

#### Observations and Interpretation:
- The majority of machines (35015 -> 92.7%) have 0.5 CPU capacity
- The analysis is relevant because only a small fraction (0.08%) have undefined (NULL) CPU values
- Machines are distributed across three main capacity tiers: 0.25, 0.5, and 1.0
---

### Q2: Computational Power Lost to Maintenance

**Question:** What is the percentage of computational power lost due to maintenance?

#### Implementation and Challenges:

To calculate the percentage of computational power lost, this are the steps:
1. Created a window partitioned by `machine_ID` and ordered by `time`
2. Used `lag()` to get previous event type, timestamp, and CPU capacity
3. Filtered for ADD events (0) that follow REMOVE events (1)
4. Calculated downtime and multiplied by effective CPU capacity
5. Compared total CPU-time lost with theoretical capacity (all machines always online)

The computational loss is proportional to both the CPU capacity and the unavailability period:
```
CPU_loss = Σ (downtime × machine_cpus)
Percentage = (CPU_loss / Total_capacity) × 100
```

**Challenges:**

Two main issues complicated the CPU capacity calculation during maintenance intervals:

1. **UPDATE events (event_type=2):** Machines can change their CPU/memory capacity during operation. This creates ambiguity about which capacity value to use when calculating loss during a REMOVE→ADD interval.

2. **Consecutive NULL values:** Using `lag(1)` to retrieve the previous CPU capacity can fail if multiple consecutive events have NULL cpus values, making even the `coalesce(prev_cpus, current_cpus)` fallback insufficient.

**Simplified Approach Adopted:**
- Used the CPU value from the immediate previous event (`lag(1)`)
- Applied `coalesce(prev_cpus, current_cpus)` as fallback for single NULL cases
- This provides a reasonable approximation but may slightly underestimate losses in cases as multiple UPDATEs or consecutive NULLs during maintenance.

#### Results:

Computational power lost to maintenance: **0.48%**

#### Observations and Interpretation:

The result shows that Google's cluster maintained excellent availability, consistent with typical high-availability datacenter targets (99%+ uptime). The low loss percentage demonstrates effective maintenance scheduling and resource management. However, it should be noted that due to the simplified assumptions and NULL data handling described above, the actual value may vary slightly from this estimation.

---

### Q3: Maintenance Rate by Machine Class

**Question:** Is there a class of machines with higher maintenance rate?

#### Implementation and Challenges:
[Brief description of approach]

#### Results:

| CPU Capacity | Machines with Downtime | Total Machines | Maintenance Rate |
|--------------|------------------------|----------------|------------------|
| 1.0          | 560                    | 798            | 70.18%           |
| 0.25         | 74                     | 126            | 58.73%           |
| 0.5          | 6,918                  | 11,659         | 59.34%           |
| NULL         | 0                      | 32             | 0.00%            |

**Key Findings:**
- Machines with 1.0 CPU capacity show the highest maintenance rate at 70.18%
- Machines with 0.5 CPU capacity (the most common) have a 59.34% maintenance rate
- Lower capacity machines (0.25) show a slightly lower rate at 58.73%

#### Observations and Interpretation:

The key findings are:
- Machines with 1.0 CPU capacity show the highest maintenance rate at 70.18%
- Machines with 0.5 CPU capacity (the most common) have a 59.34% maintenance rate
- Lower capacity machines (0.25) show a slightly lower rate at 58.73%

This finding aligns with expectations, as machines with 1.0 CPU capacity represent the highest-resource tier in the cluster and likely experience greater workload intensity, leading to increased maintenance requirements. It's important to note that the CPU capacity values (0.25, 0.5, 1.0) are normalized relative to the maximum capacity in the cluster (1.0), as specified in the Google Cluster Trace documentation. The relatively similar maintenance rates between 0.25 and 0.5 capacity machines (58.73% vs 59.34%) suggest that maintenance frequency may be influenced more by factors beyond normalized capacity, such as workload patterns, hardware age, or operational policies applied uniformly across lower-tier machines. DA RIVEDEREEEEEEEE

---

### Q4: Task Resource Request Analysis

**Question:** What is the distribution of resource requests (CPU, memory, disk) across tasks?

#### Implementation and Challenges:
[Brief description of approach]

#### Results:
+----------------+---------+------+
|scheduling_class|    tasks|  jobs|
+----------------+---------+------+
|               0|114598888|771302|
|               1| 18055014|643916|
|               2| 11158598|582340|
|               3|   835788| 14684|
+----------------+---------+------+

#### Observations and Interpretation:
[Your analysis here]

---

### Q5: Task Execution Time Analysis

**Question:** 
Would you qualify the percentage of jobs/tasks that got killed or evicted as important?

#### Implementation and Challenges:
[Brief description of approach]

#### Results:

Percentage of Job killed or Evicted: 40.53%
Percentage of Task Killed or Evicted: 32.14%

#### Observations and Interpretation:
[Your analysis here]

---

### Q6: Job Completion Patterns

**Question:** 
Do tasks with a low scheduling class have a higher probability of being evicted?

#### Implementation and Challenges:
[Brief description of approach]

#### Results:
Percentuale tasks evicted / total low scheduling class tasks: 5.59%
Percentuale tasks evicted / total high scheduling class tasks: 7.40%

#### Observations and Interpretation:
[Your analysis here]

---

### Q7: Resource Utilization Efficiency

**Question:** 
In general, do tasks from the same job run on the same machine? Comment on the observed
locality strategy and its pros and cons

#### Implementation and Challenges:
[Brief description of approach]

#### Results:
+----------+------------------+--------------------------+---------------------+
|    job_ID|total_task_per_job|distinct_machines_per_task|locality_distribution|
+----------+------------------+--------------------------+---------------------+
|6261818848|              5943|                      1753|   29.496887094060238|
|6334544871|              3036|                       841|   27.700922266139656|
|6334525273|               300|                       100|    33.33333333333333|
|6334459620|              2994|                       996|    33.26653306613226|
|6275512898|              3036|                      1003|     33.0368906455863|
|6276103240|                54|                        18|    33.33333333333333|
|6276226189|             45771|                      7083|   15.474863996853902|
|6345351634|              3177|                      1050|   33.050047214353164|
|6323791794|              2679|                       852|    31.80291153415454|
|6399112180|               168|                        50|   29.761904761904763|
|6432966329|              1200|                       389|   32.416666666666664|
|6268151907|              7482|                      2244|   29.991980753809145|
|6376286391|              1503|                       497|    33.06719893546241|
|6406577194|                45|                        15|    33.33333333333333|
|6333178566|              1119|                       320|   28.596961572832885|
|6323064630|              2670|                       849|   31.797752808988765|
|6375784491|             19758|                      4820|   24.395181698552484|
|6133390513|             17347|                      4031|    23.23744739724448|
|6471108647|              5439|                      1683|   30.943188086045232|
|6436049890|                69|                        23|    33.33333333333333|
+----------+------------------+--------------------------+---------------------+
only showing top 20 rows


#### Observations and Interpretation:
[Your analysis here]

---

### Q8: Resource Request vs. Consumption

**Question:** Are the tasks that request more resources the ones that consume more resources?

**Implementation:**
[Brief description of approach]

**Results:**

#### CPU Analysis

**Top 10 CPU Requesters:**
| job_ID | task_index | CPU Request | CPU Consumption |
|--------|------------|-------------|-----------------|
|        |            |             |                 |

**Top 10 CPU Consumers:**
| job_ID | task_index | CPU Request | CPU Consumption |
|--------|------------|-------------|-----------------|
|        |            |             |                 |

#### Memory Analysis
[Similar tables]

#### Disk Analysis
[Similar tables]

**Interpretation:**

[Your analysis here]

---

### Q9: Resource Waste and Over-Provisioning

**Question:** 
Can we observe correlations between peaks of high resource consumption on some machines and task eviction events?

#### Implementation and Challenges:
[Brief description of approach]

#### Results:


#### Observations and Interpretation:
[Your analysis here]

---

### Q10: Resource Overcommitment Frequency

**Question:** How often does it happen that the resources of a machine are over-committed?

**Implementation:**
[Brief description of approach]

**Results:**

| Metric                          | Percentage |
|---------------------------------|------------|
| CPU overcommitment frequency    |            |
| Memory overcommitment frequency |            |
| Either resource overcommitted   |            |

**Interpretation:**

[Your analysis here]

---

### Q11: Machine Heterogeneity and Its Impact

**Question:** How heterogeneous is the cluster in terms of machine characteristics and how does this impact resource allocation?

#### Implementation and Challenges:
[Brief description of approach]

#### Results:

| Metric | Value |
|--------|-------|
| Number of distinct machine configurations | |
| CPU capacity variants | |
| Memory capacity variants | |
| Machine distribution across configurations | |
| Average tasks per machine type | |

#### Observations and Interpretation:
[Your analysis here]

---

### Q12: Task Reschedule Success and Priority Influence (Original Question)

**Question:** What proportion of tasks complete successfully on their first scheduling attempt versus requiring multiple reschedule cycles? Does task priority significantly influence reschedule rates?

**Motivation:**

[Why this question extends the course requirements]

**Implementation:**
[Brief description of approach]

**Results:**

#### Overall Reschedule Distribution

| Reschedule Category | Task Count | Percentage |
|---------------------|------------|------------|
| First attempt       |            |            |
| 1 reschedule        |            |            |
| 2-4 reschedules     |            |            |
| 5-10 reschedules    |            |            |
| 10+ reschedules     |            |            |

**First-Attempt Success Rate:** ___

#### Reschedule Distribution by Priority Group

| Priority Group | First Attempt | 1 Reschedule | 2-4 | 5-10 | 10+ |
|----------------|---------------|--------------|-----|------|-----|
| Free tier (0)  |               |              |     |      |     |
| Low (1)        |               |              |     |      |     |
| Medium (2,3,5) |               |              |     |      |     |
| Production (4) |               |              |     |      |     |
| High (6-11)    |               |              |     |      |     |

**Interpretation:**

[Your analysis here]

---

## 4. Conclusions

**Key Findings:**
1. [Finding from Q1]
2. [Finding from Q2]
3. [Finding from Q3]
4. [Finding from Q8]
5. [Finding from Q10]
6. [Finding from Q12]

**Lessons Learned:**
- [Technical insight 1]
- [Technical insight 2]

---

## References

- Google Cluster Trace Documentation: https://github.com/google/cluster-data
- Apache Spark Documentation: https://spark.apache.org/docs/latest/