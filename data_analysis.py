from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark import SparkContext, SparkConf

import time
from timeit import default_timer as timer

# ============================================================================
# DATA LOADING FUNCTIONS
# ============================================================================


def load_machine_events(spark, path, do_cast=True):
    """
    Load machine events data.

    Args:
        spark: SparkSession object
        path: Path to machine events CSV file

    Returns:
        DataFrame: Machine events data
    """
    me_cols = ["time", "machine_ID", "event_type", "platform_id", "cpus", "memory"]
    df = spark.read.csv(path, header=False, inferSchema=False).toDF(*me_cols)
    df = df.repartition(200)

    if do_cast:
        df = df.select(
            F.col("time").cast("long").alias("time"),
            F.col("machine_ID").cast("long").alias("machine_ID"),
            F.col("event_type").cast("int").alias("event_type"),
            F.col("platform_id"),
            F.col("cpus").cast("float").alias("cpus"),
            F.col("memory").cast("float").alias("memory"),
        )
    return df


def load_machine_attributes(spark, path):
    """
    Load machine attributes data.

    Args:
        spark: SparkSession object
        path: Path to machine attributes CSV file

    Returns:
        DataFrame: Machine attributes data
    """
    return spark.read.csv(path, header=True, inferSchema=True)


def load_job_events(spark, path):
    """
    Load job events data.

    Args:
        spark: SparkSession object
        path: Path to job events CSV file

    Returns:
        DataFrame: Job events data
    """
    je_cols = [
        "time",
        "missing_info",
        "job_ID",
        "event_type",
        "user",
        "scheduling_class",
        "job_name",
        "logical_job_name",
    ]
    df = spark.read.csv(path, header=False, inferSchema=False)
    df = df.repartition(200)
    return df.toDF(*je_cols)


def load_task_events(spark, path, do_cast=True):
    """
    Load task events data.

    Args:
        spark: SparkSession object
        path: Path to task events CSV file
        do_cast: cast columns to expected types (default True)

    Returns:
        DataFrame: Task events data
    """
    te_cols = [
        "time",
        "missing_info",
        "job_ID",
        "task_index",
        "machine_ID",
        "event_type",
        "user",
        "scheduling_class",
        "priority",
        "CPU_request",
        "memory_request",
        "disk_space_request",
        "different_machine_restrictions",
    ]
    df = spark.read.csv(path, header=False, inferSchema=False)
    df = df.toDF(*te_cols)
    df = df.repartition(200)

    if do_cast:
        df = df.select(
            F.col("time").cast("long").alias("time"),
            F.col("missing_info").cast("int").alias("missing_info"),
            F.col("job_ID").cast("long").alias("job_ID"),
            F.col("task_index").cast("long").alias("task_index"),
            F.col("machine_ID").cast("long").alias("machine_ID"),
            F.col("event_type").cast("int").alias("event_type"),
            F.col("user"),
            F.col("scheduling_class").cast("int").alias("scheduling_class"),
            F.col("priority").cast("int").alias("priority"),
            F.col("CPU_request").cast("float").alias("CPU_request"),
            F.col("memory_request").cast("float").alias("memory_request"),
            F.col("disk_space_request").cast("float").alias("disk_space_request"),
            F.col("different_machine_restrictions"),
        )
    return df


def load_task_usage(spark, path, do_cast=True):
    """
    Load task usage data.

    Args:
        spark: SparkSession object
        path: Path to task usage CSV file
        do_cast: cast columns to expected types (default True)

    Returns:
        DataFrame: Task usage data
    """

    tu_cols = [
        "start_time",
        "end_time",
        "job_ID",
        "task_index",
        "machine_ID",
        "CPU_rate",
        "canonical_mem_usage",
        "assigned_mem_usage",
        "unmap_page_cache",
        "total_page_cache",
        "max_mem_usage",
        "io_time",
        "local_disk_space_usage",
        "max_CPU_rate",
        "max_io_time",
        "cycles_per_instruction",
        "mem_accesses_per_instruction",
        "sample_portion",
        "aggregation_type",
        "sampled_CPU_usage",
    ]

    df = spark.read.csv(path, header=False, inferSchema=False).toDF(*tu_cols)
    df = df.repartition(200)

    if do_cast:
        df = df.select(
            F.col("start_time").cast("long").alias("start_time"),
            F.col("end_time").cast("long").alias("end_time"),
            F.col("job_ID").cast("long").alias("job_ID"),
            F.col("task_index").cast("long").alias("task_index"),
            F.col("machine_ID").cast("long").alias("machine_ID"),
            F.col("CPU_rate").cast("float").alias("CPU_rate"),
            F.col("canonical_mem_usage").cast("float").alias("canonical_mem_usage"),
            F.col("assigned_mem_usage").cast("float").alias("assigned_mem_usage"),
            F.col("unmap_page_cache").cast("float").alias("unmap_page_cache"),
            F.col("total_page_cache").cast("float").alias("total_page_cache"),
            F.col("max_mem_usage").cast("float").alias("max_mem_usage"),
            F.col("io_time").cast("float").alias("io_time"),
            F.col("local_disk_space_usage")
            .cast("float")
            .alias("local_disk_space_usage"),
            F.col("max_CPU_rate").cast("float").alias("max_CPU_rate"),
            F.col("max_io_time").cast("float").alias("max_io_time"),
            F.col("cycles_per_instruction")
            .cast("float")
            .alias("cycles_per_instruction"),
            F.col("mem_accesses_per_instruction")
            .cast("float")
            .alias("mem_accesses_per_instruction"),
            F.col("sample_portion").cast("float").alias("sample_portion"),
            F.col("aggregation_type"),
            F.col("sampled_CPU_usage").cast("float").alias("sampled_CPU_usage"),
        )
    return df


def load_schema(spark, path):
    """
    Load schema information.

    Args:
        spark: SparkSession object
        path: Path to schema CSV file

    Returns:
        DataFrame: Schema data
    """
    return spark.read.csv(path, header=True, inferSchema=True)


def analysis_1_cpu_distribution(machine_events_df):
    """
    Q1: What is the distribution of machines according to their CPU capacity?

    Args:
        machine_events_df: Machine events DataFrame

    Returns:
        DataFrame: CPU capacity distribution results
    """
    # Group machines by CPU capacity and count how many machines have each CPU value
    machine_events_df.groupBy(["cpus"]).count().show()
    pass


def analysis_2_maintenance_loss(machine_events_df):
    """
    Q2: What is the percentage of computational power lost due to maintenance?

    Args:
        machine_events_df: Machine events DataFrame

    Returns:
        float: Percentage of computational power lost
    """
    # Create window for per-machine operations ordered by time
    w = Window.partitionBy("machine_ID").orderBy("time")

    # Get previous event information using lag to find REMOVE->ADD transitions
    df = (
        machine_events_df.withColumn("prev_event", F.lag("event_type", 1).over(w))
        .withColumn("prev_time", F.lag("time", 1).over(w))
        .withColumn("prev_cpus", F.lag("cpus", 1).over(w))
    )

    # Use previous CPU value, or current if previous is null
    df1 = df.withColumn(
        "effective_prev_cpus", F.coalesce(F.col("prev_cpus"), F.col("cpus"))
    )

    # Find ADD events (0) that follow REMOVE events (1) - these are maintenance intervals
    adds_after_remove = df1.filter(
        (F.col("event_type") == 0)
        & (F.col("prev_event") == 1)
        & (F.col("prev_time").isNotNull())
    )

    # Calculate downtime and CPU loss for each maintenance interval
    adds_with_loss = adds_after_remove.withColumn(
        "downtime", F.col("time") - F.col("prev_time")
    ).withColumn("cpu_loss", F.col("downtime") * F.col("effective_prev_cpus"))

    # Sum total CPU-time lost across all maintenance intervals
    total_cpu_lost = (
        adds_with_loss.agg(F.sum("cpu_loss").alias("total_cpu_lost")).collect()[0][
            "total_cpu_lost"
        ]
        or 0.0
    )

    # Get maximum timestamp for observation window
    max_t = machine_events_df.agg(F.max("time")).first()[0]

    # Get first appearance time and CPU capacity for each machine
    first_appearance = machine_events_df.groupBy("machine_ID").agg(
        F.min("time").alias("first_time"), F.first("cpus").alias("cpus")
    )

    # Calculate total capacity per machine
    capacity_per_machine = first_appearance.withColumn(
        "available_time", max_t - F.col("first_time")
    ).withColumn("cpu_capacity", F.col("available_time") * F.col("cpus"))

    # Sum total theoretical CPU-time capacity if all machines were always online
    total_possible = (
        capacity_per_machine.agg(F.sum("cpu_capacity")).collect()[0][
            "sum(cpu_capacity)"
        ]
        or 0.0
    )

    # Calculate percentage
    pct_lost = (total_cpu_lost / total_possible * 100.0) if total_possible > 0 else 0.0

    print(pct_lost)

    pass


def analysis_3_maintenance_by_class(machine_events_df):
    """
    Q3: Is there a class of machines with higher maintenance rate?

    Args:
        machine_events_df: Machine events DataFrame

    Returns:
        DataFrame: Maintenance rate by machine class
    """

    # Create window for per-machine operations ordered by time
    w = Window.partitionBy("machine_ID").orderBy("time")
    # Get previous event type using lag to identify maintenance transitions
    df = machine_events_df.withColumn("prev_event", F.lag("event_type", 1).over(w))

    # Group by CPU class and calculate maintenance statistics
    df = (
        df.groupBy("cpus").agg(
            # Count REMOVE events (1) that follow ADD events (0) - these are maintenance occurrences
            F.sum(
                F.when(
                    (F.col("event_type") == 1) & (F.col("prev_event") == 0), 1
                ).otherwise(0)
            ).alias("num_down"),
            # Count distinct machines in each CPU class
            F.countDistinct("machine_ID").alias("num_machines"),
        )
        # Calculate maintenance rate: average maintenance events per machine in each class
        .withColumn("maintenance_rate", F.col("num_down") / F.col("num_machines"))
    )

    df.show()

    pass


def analysis_4_jobs_tasks_distribution(job_events, task_events):
    """
    Q4: Distribution per scheduling class (Side-by-side comparison).
    Output columns: scheduling_class | tasks | jobs
    """
    
    #Count of Jobs
    jobs_dist = job_events.groupBy("scheduling_class").count() \
                          .withColumnRenamed("count", "jobs")

    #Count of task 
    tasks_dist = task_events.groupBy("scheduling_class").count() \
                            .withColumnRenamed("count", "tasks")

    #Join between jobs and tasks
    final_df = jobs_dist.join(tasks_dist, "scheduling_class", "outer").na.fill(0)

    #Final results shown
    result = final_df.select("scheduling_class", "tasks", "jobs") \
                     .orderBy(F.asc("scheduling_class"))
    result.show() 
pass


def analysis_5_killed_evicted_percentage(job_events, task_events):
    """
    Q5: Percentage of jobs/tasks that got killed or evicted.
    
    Optimized version for cloud execution:
    - Reduces columns immediately to save RAM (Column Pruning)
    - Uses count_distinct() for efficient aggregation
    - Avoids multiple full dataframe scans
    """
    
    # Create a lightweight dataframe with only the columns we need from job_events
    jobs_lite = job_events.select(
        F.col("job_ID"),                                    
        F.col("event_type").cast("int").alias("type_int")  
    )
    
    # Calculate total number of unique jobs in the dataset
    total_job_events = jobs_lite.agg(F.count_distinct("job_ID")).collect()[0][0]
    
    # Count jobs that were either Killed (event_type=5) or Evicted (event_type=2)
    count_job_killed = jobs_lite.filter(
        (F.col("type_int") == 5) | (F.col("type_int") == 2)  
    ).agg(F.count_distinct("job_ID")).collect()[0][0]        

    
    tasks_lite = task_events.select(
        F.col("job_ID"),                                    
        F.col("task_index"),                              
        F.col("event_type").cast("int").alias("type_int")  
    )

    # Calculate total number of unique tasks in the dataset
    total_task_events = tasks_lite.agg(
        F.count_distinct("job_ID", "task_index")
    ).collect()[0][0]

    # Count tasks that were either Killed or Evicted
    count_task_killed = tasks_lite.filter(
        (F.col("type_int") == 5) | (F.col("type_int") == 2) 
    ).agg(
        F.count_distinct("job_ID", "task_index")  
    ).collect()[0][0]


    # Report on job events: print percentage of jobs that were killed/evicted
    if count_job_killed == 0:
        print("No job has been killed or evicted")
    else:
        # Calculate percentage: (killed_jobs / total_jobs) * 100
        percentage_job = (count_job_killed / total_job_events) * 100
        print(f"Percentuale Job killed or Evicted: {percentage_job:.2f}%")

    # Report on task events: print percentage of tasks that were killed/evicted
    if count_task_killed == 0:
        print("No task has been killed or evicted")
    else:
        # Calculate percentage: (killed_tasks / total_tasks) * 100
        percentage_task = (count_task_killed / total_task_events) * 100
        print(f"Percentuale Task Killed or Evicted: {percentage_task:.2f}%")

    return {
        "job_percentage": percentage_job if count_job_killed > 0 else 0,
        "task_percentage": percentage_task if count_task_killed > 0 else 0
    }
pass


def analysis_6_eviction_by_scheduling_class(task_events):
    """
    Q6: Do tasks with low scheduling class have higher eviction probability?

    Args:
        task_events_df: Task events DataFrame

    Returns:
        DataFrame: Eviction probability by scheduling class
    """
    task_events_low_scheduling_class = task_events.filter(
        task_events.scheduling_class < 3
    )
    total_events_low_scheduling_class = (
        task_events_low_scheduling_class.select("job_ID", "task_index")
        .distinct()
        .count()
    )
    task_events_high_scheduling_class = task_events.filter(
        task_events.scheduling_class == 3
    )
    total_events_high_scheduling_class = (
        task_events_high_scheduling_class.select("job_ID", "task_index")
        .distinct()
        .count()
    )

    count_evicted_low = (
        task_events_low_scheduling_class.filter(
            task_events_low_scheduling_class.event_type == 2
        )
        .select("job_ID", "task_index")
        .distinct()
        .count()
    )
    count_evicted_high = (
        task_events_high_scheduling_class.filter(
            task_events_high_scheduling_class.event_type == 2
        )
        .select("job_ID", "task_index")
        .distinct()
        .count()
    )

    if count_evicted_low == 0:
        print("No task has been evicted from the low scheduling class")
    else:
        percentage_job = (count_evicted_low / total_events_low_scheduling_class) * 100
        print(
            f"Percentuale tasks evicted / total low scheduling class tasks: {percentage_job:.2f}%"
        )

    if count_evicted_high == 0:
        print("No task has been evicted from the high scheduling class")
    else:
        percentage_job = (count_evicted_high / total_events_high_scheduling_class) * 100
        print(
            f"Percentuale tasks evicted / total high scheduling class tasks: {percentage_job:.2f}%"
        )
pass


def analysis_7_task_locality(task_events):
    """
    Q7: Do tasks from the same job run on the same machine?

    Args:
        task_events_df: Task events DataFrame

    Returns:
        DataFrame: Locality analysis results
    """
    task_events_same_jobs = task_events.groupBy("job_ID").agg(
        F.count(task_events.task_index).alias("total_task_per_job"),
        F.count_distinct(task_events.machine_ID).alias("distinct_machines_per_task"),
    )

    locality_distribution = task_events_same_jobs.select(
        "*",
        (F.col("distinct_machines_per_task") / F.col("total_task_per_job") * 100).alias(
            "locality_distribution"
        ),
    )

    print("\n---Locality distribution of tasks--- ")
    locality_distribution.show()
pass


def analysis_8_resource_request_vs_consumption(task_events_df, task_usage_df):
    """
    Q8: Do tasks requesting more resources consume more resources?

    Args:
        task_events_df: Task events DataFrame
        task_usage_df: Task usage DataFrame

    Returns:
        DataFrame: Correlation analysis results
    """

    # Aggregate maximum resource requests per task from task_events
    task_requests = task_events_df.groupBy("job_ID", "task_index").agg(
        F.max("CPU_request").alias("max_cpu_request"),
        F.max("memory_request").alias("max_memory_request"),
        F.max("disk_space_request").alias("max_disk_request"),
    )

    # Aggregate maximum actual resource consumption per task from task_usage
    task_consumption = task_usage_df.groupBy("job_ID", "task_index").agg(
        F.max("max_CPU_rate").alias("max_cpu_consumption"),
        F.max("max_mem_usage").alias("max_memory_consumption"),
        F.max("local_disk_space_usage").alias("max_disk_consumption"),
    )

    # Join requests and consumption data to compare side-by-side
    task_analysis = task_requests.join(
        task_consumption, on=["job_ID", "task_index"], how="inner"
    )

    print("\n--- CPU Analysis ---")

    # Find top 10 tasks that requested the most CPU
    top_cpu_requesters = (
        task_analysis.orderBy(F.desc("max_cpu_request"))
        .limit(10)
        .select("job_ID", "task_index", "max_cpu_request", "max_cpu_consumption")
    )

    print("Top 10 CPU Requesters:")
    top_cpu_requesters.show()

    # Find top 10 tasks that actually consumed the most CPU
    top_cpu_consumers = (
        task_analysis.orderBy(F.desc("max_cpu_consumption"))
        .limit(10)
        .select("job_ID", "task_index", "max_cpu_request", "max_cpu_consumption")
    )

    print("Top 10 CPU Consumers:")
    top_cpu_consumers.show()

    print("\n--- Memory Analysis ---")

    # Find top 10 tasks that requested the most memory
    top_memory_requesters = (
        task_analysis.orderBy(F.desc("max_memory_request"))
        .limit(10)
        .select("job_ID", "task_index", "max_memory_request", "max_memory_consumption")
    )

    print("Top 10 Memory Requesters:")
    top_memory_requesters.show()

    # Find top 10 tasks that actually consumed the most memory
    top_memory_consumers = (
        task_analysis.orderBy(F.desc("max_memory_consumption"))
        .limit(10)
        .select("job_ID", "task_index", "max_memory_request", "max_memory_consumption")
    )

    print("Top 10 Memory Consumers:")
    top_memory_consumers.show()

    print("\n--- Disk Analysis ---")

    # Find top 10 tasks that requested the most disk space
    top_disk_requesters = (
        task_analysis.orderBy(F.desc("max_disk_request"))
        .limit(10)
        .select("job_ID", "task_index", "max_disk_request", "max_disk_consumption")
    )

    print("Top 10 Disk Requesters:")
    top_disk_requesters.show()

    # Find top 10 tasks that actually consumed the most disk space
    top_disk_consumers = (
        task_analysis.orderBy(F.desc("max_disk_consumption"))
        .limit(10)
        .select("job_ID", "task_index", "max_disk_request", "max_disk_consumption")
    )

    print("Top 10 Disk Consumers:")
    top_disk_consumers.show()
pass


def analysis_9_consumption_peaks_vs_eviction(machine_events, task_events, task_usage):
    """
    Q9: Correlation between resource consumption peaks and task evictions.

    Args:
        machine_events_df: Machine events DataFrame
        task_events_df: Task events DataFrame
        task_usage_df: Task usage DataFrame

    Note:
    - in the task_usage table, the datas about resources are not normalized, (es: 2 means 2 core used).
      While, for the task_events e machine_events datas about resources are normalized between 0 and 1
    - in task_usage i dati sono aggreagati ogni 5 minuti

    Returns:
        DataFrame: Correlation results
    """
    evicted_tasks = (
        task_events.filter(F.col("event_type") == 2)
        .withColumn("window_time", (F.col("time") / 300000000).cast("long") * 300000000)
        .select("machine_ID", "window_time")
        .distinct()
        .withColumn("has_eviction", F.lit(1))
    )

    machine_peaks = (
        task_usage.groupBy("machine_ID", "start_time")
        .agg(
            F.sum("max_CPU_rate").alias("total_cpu_consumption"),
            F.max("max_mem_usage").alias("total_memory_consumption"),
            F.max("local_disk_space_usage").alias("total_disk_consumption"),
        )
        .repartition(100, "machine_ID")
    )

    correlation_evicted_and_peaks = (
        machine_peaks.join(
            evicted_tasks,
            (machine_peaks.machine_ID == evicted_tasks.machine_ID)
            & (machine_peaks.start_time == evicted_tasks.window_time),
            "left",
        )
        .fillna(0, subset=["has_eviction"])
        .persist()
    )

    cpu_correlation = correlation_evicted_and_peaks.stat.corr(
        "total_cpu_consumption", "has_eviction"
    )
    mem_correlation = correlation_evicted_and_peaks.stat.corr(
        "total_memory_consumption", "has_eviction"
    )

    print(
        f"Correlation between peaks of high cpu consuption on some machines and task eviction events: {cpu_correlation}"
    )
    print(
        f"Correlation between peaks of high memory consuption on some machines and task eviction events: {mem_correlation}"
    )
pass


def analysis_10_overcommitment_frequency(machine_events_df, task_events_df):
    """
    Q10: How often are machine resources over-committed?

    Args:
        machine_events_df: Machine events DataFrame
        task_events_df: Task events DataFrame
        task_usage_df: Task usage DataFrame

    Returns:
        DataFrame: Overcommitment frequency results
    """

    # Create window for per-task operations to track resource request changes
    task_win = Window.partitionBy("job_ID", "task_index").orderBy("time")

    # Create window for cumulative sum of resource deltas per machine over time
    machine_win = (
        Window.partitionBy("machine_ID")
        .orderBy("time")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    # Create window for global per-machine operations to find minimum values
    global_machine_win = Window.partitionBy("machine_ID")

    # Get previous resource requests for each task to calculate deltas on UPDATE events
    df_deltas = task_events_df.withColumn(
        "prev_cpu_req", F.lag("CPU_request").over(task_win)
    ).withColumn("prev_mem_req", F.lag("memory_request").over(task_win))

    # Calculate resource deltas based on event type: +request on SCHEDULE, delta on UPDATE, -request on terminal events
    df_deltas = df_deltas.withColumn(
        "delta_cpu",
        F.when(F.col("event_type") == 1, F.col("CPU_request"))
        .when(F.col("event_type") == 8, F.col("CPU_request") - F.col("prev_cpu_req"))
        .when(F.col("event_type").isin(2, 3, 4, 5, 6), -F.col("CPU_request"))
        .otherwise(0.0),
    ).withColumn(
        "delta_mem",
        F.when(F.col("event_type") == 1, F.col("memory_request"))
        .when(F.col("event_type") == 8, F.col("memory_request") - F.col("prev_mem_req"))
        .when(F.col("event_type").isin(2, 3, 4, 5, 6), -F.col("memory_request"))
        .otherwise(0.0),
    )

    # Calculate cumulative resource load per machine over time (raw values may be negative)
    df_raw = df_deltas.withColumn(
        "raw_cpu", F.sum("delta_cpu").over(machine_win)
    ).withColumn("raw_mem", F.sum("delta_mem").over(machine_win))

    # Normalize to zero baseline by subtracting minimum value per machine
    df_load = (
        df_raw.withColumn("min_cpu", F.min("raw_cpu").over(global_machine_win))
        .withColumn("min_mem", F.min("raw_mem").over(global_machine_win))
        .withColumn("total_cpu_req", F.col("raw_cpu") - F.col("min_cpu"))
        .withColumn("total_mem_req", F.col("raw_mem") - F.col("min_mem"))
    )

    # Prepare machine capacity data with renamed columns for joining
    m_events = machine_events_df.select(
        F.col("time").alias("m_time"),
        F.col("machine_ID"),
        F.col("cpus").alias("m_cpus"),
        F.col("memory").alias("m_mem"),
    )

    # Join with machine capacity, keeping only events after machine was added
    df_joined = df_load.join(m_events, "machine_ID").filter(
        F.col("time") >= F.col("m_time")
    )

    # Create window to find most recent machine capacity for each task event
    join_win = Window.partitionBy("machine_ID", "time", "job_ID", "task_index").orderBy(
        F.col("m_time").desc()
    )

    # Keep only the most recent machine capacity entry for each task event
    df_with_cap = (
        df_joined.withColumn("rank", F.row_number().over(join_win))
        .filter(F.col("rank") == 1)
        .drop("rank", "m_time")
    )

    # Flag overcommitment cases where cumulative load exceeds machine capacity
    df_over = (
        df_with_cap.withColumn(
            "is_over_cpu",
            F.when(F.col("total_cpu_req") > F.col("m_cpus"), 1).otherwise(0),
        )
        .withColumn(
            "is_over_mem",
            F.when(F.col("total_mem_req") > F.col("m_mem"), 1).otherwise(0),
        )
        .withColumn(
            "is_over_gen",
            F.when(
                (F.col("is_over_cpu") == 1) | (F.col("is_over_mem") == 1), 1
            ).otherwise(0),
        )
    )

    # Calculate percentage of events where resources were overcommitted
    result = df_over.select(
        (F.mean("is_over_cpu") * 100).alias("percentage_over_cpu"),
        (F.mean("is_over_mem") * 100).alias("percentage_over_mem"),
        (F.mean("is_over_gen") * 100).alias("percentage_over_general"),
    )

    result.show()

pass


# ============================================================================
# ORIGINAL ANALYSES
# ============================================================================


def analysis_11_user_task(task_events):
    """
    Q11: Can repeatedly rescheduled tasks be identified at the user level, and do these
    rescheduling events occur at consistent time intervals, indicating systematic
    resource exhaustion rather than random preemption?

    Returns:
        DataFrame: Task-level analysis with reschedule patterns
    """

    # Filter valid events (remove zero timestamps)
    task_events_clean = task_events.filter(F.col("time") > 0)

    # Define window for tracking event sequences per task
    window_spec = Window.partitionBy("user", "job_ID", "task_index").orderBy("time")

    # Track previous event and timestamp to identify SCHEDULE -> EVICT patterns
    df_history = task_events_clean.withColumn(
        "prev_event", F.lag("event_type").over(window_spec)
    ).withColumn("prev_time", F.lag("time").over(window_spec))

    # Identify EVICT (2) events following SCHEDULE (1) events
    evictions = df_history.filter(
        (F.col("event_type") == 2) & (F.col("prev_event") == 1)
    ).withColumn("stability_min", (F.col("time") - F.col("prev_time")) / 60000000)

    # Aggregate by user and task to identify repeatedly rescheduled tasks
    task_summary = (
        evictions.groupBy("user", "job_ID", "task_index")
        .agg(
            F.count("*").alias("num_reschedules"),
            F.avg("stability_min").alias("avg_stability_min"),
            F.stddev("stability_min").alias("stddev_stability_min"),
            F.min("stability_min").alias("min_stability_min"),
            F.max("stability_min").alias("max_stability_min"),
            F.first("priority").alias("priority"),
        )
        .filter(F.col("num_reschedules") > 1)
    )

    # Categorize reschedule patterns based on temporal consistency
    task_summary = task_summary.withColumn(
        "pattern",
        F.when(
            (F.col("stddev_stability_min") < (F.col("avg_stability_min") * 0.2))
            | ((F.col("max_stability_min") - F.col("min_stability_min")) < 1.0),
            "consistent",
        ).otherwise("variable"),
    )

    # Cache for reuse in multiple aggregations
    task_summary.cache()

    print("\n=== TOP 20 TASKS WITH MOST RESCHEDULES ===")
    task_summary.orderBy(F.desc("num_reschedules")).show(20, truncate=False)

    # User-level aggregation to identify problematic users
    user_summary = (
        task_summary.groupBy("user")
        .agg(
            F.count("*").alias("problematic_tasks"),
            F.sum("num_reschedules").alias("total_reschedules"),
            F.avg("avg_stability_min").alias("avg_uptime_before_eviction_min"),
        )
        .orderBy(F.desc("total_reschedules"))
    )

    print("\n=== TOP 20 USERS WITH RESCHEDULE ISSUES ===")
    user_summary.show(20, truncate=False)

    # Pattern distribution analysis
    pattern_dist = (
        task_summary.groupBy("pattern")
        .agg(F.count("*").alias("task_count"))
        .withColumn(
            "percentage",
            F.round(
                F.col("task_count")
                * 100.0
                / F.sum("task_count").over(Window.partitionBy()),
                2,
            ),
        )
    )

    print("\n=== RESCHEDULE PATTERN DISTRIBUTION ===")
    print("Consistent: Rescheduling at similar intervals → Resource exhaustion likely")
    print("Variable: Rescheduling at varying intervals → Preemption likely")
    pattern_dist.show()

    # Summary statistics
    total_tasks = task_summary.count()
    consistent_tasks = task_summary.filter(F.col("pattern") == "consistent").count()
    variable_tasks = task_summary.filter(F.col("pattern") == "variable").count()

    print(f"Total repeatedly rescheduled tasks: {total_tasks}")
    print(
        f"  Consistent pattern: {consistent_tasks} ({consistent_tasks*100/total_tasks:.1f}%)"
    )
    print(f"    → Systematic resource exhaustion")
    print(
        f"  Variable pattern: {variable_tasks} ({variable_tasks*100/total_tasks:.1f}%)"
    )
    print(f"    → Random preemption by higher-priority tasks")

    return task_summary.orderBy(F.desc("num_reschedules"))
pass


def analysis_12_task_reschedule_and_priority_influence(task_events):
    """
    Q12: What proportion of tasks complete successfully on their first scheduling attempt
    versus requiring multiple reschedule cycles? Does task priority significantly influence reschedule rates?

    Motivation: Understanding reschedule patterns reveals scheduler efficiency and infrastructure stability.
    While the professor's questions examine eviction rates by scheduling class (Q6), this analysis focuses
    on priority's impact on overall reschedule success - a distinct metric that measures placement quality
    rather than just eviction probability. Priority directly influences scheduler decisions, making it a
    key factor in task stability. Additionally, identifying extreme reschedule cases helps pinpoint
    systemic issues or pathological workloads.

    Returns:
        DataFrame: Analysis results with reschedule distributions
    """

    # Filter out invalid events with zero timestamp
    task_events_clean = task_events.filter(F.col("time") > 0)

    # Identify tasks with LOST events (6) to exclude them from analysis
    tasks_with_lost = (
        task_events_clean.filter(F.col("event_type") == 6)
        .select("job_ID", "task_index")
        .distinct()
    )

    # Remove tasks with LOST events using left anti join
    task_events_clean = task_events_clean.join(
        tasks_with_lost, on=["job_ID", "task_index"], how="left_anti"
    )

    # Count SCHEDULE events (1) per task to determine how many times each task was scheduled
    schedule_counts = (
        task_events_clean.filter(F.col("event_type") == 1)
        .groupBy("job_ID", "task_index")
        .agg(
            F.count("*").alias("schedule_count"),
            F.first("priority").alias("priority"),
            F.first("scheduling_class").alias("scheduling_class"),
        )
    )

    # Filter terminal events: FAIL (3), FINISH (4), KILL (5)
    terminal_events = task_events_clean.filter(F.col("event_type").isin([3, 4, 5]))

    # Create window to find the last terminal event for each task
    window_last = Window.partitionBy("job_ID", "task_index").orderBy(F.desc("time"))

    # Get the final outcome (most recent terminal event) for each task
    final_outcomes = (
        terminal_events.withColumn("rn", F.row_number().over(window_last))
        .filter(F.col("rn") == 1)
        .select("job_ID", "task_index", F.col("event_type").alias("final_event_type"))
    )

    # Join schedule counts with final outcomes to analyze reschedule patterns
    task_analysis = schedule_counts.join(
        final_outcomes, on=["job_ID", "task_index"], how="inner"
    )

    # Map terminal event types to human-readable outcomes
    task_analysis = task_analysis.withColumn(
        "final_outcome",
        F.when(F.col("final_event_type") == 3, "FAIL")
        .when(F.col("final_event_type") == 4, "FINISH")
        .when(F.col("final_event_type") == 5, "KILL"),
    )

    # Cache for reuse in multiple aggregations
    task_analysis.cache()

    # Categorize tasks by number of reschedule attempts
    task_analysis = task_analysis.withColumn(
        "reschedule_category",
        F.when(F.col("schedule_count") == 1, "first_attempt")
        .when(F.col("schedule_count") == 2, "1_reschedule")
        .when(F.col("schedule_count").between(3, 5), "2-4_reschedules")
        .when(F.col("schedule_count").between(6, 10), "5-10_reschedules")
        .when(F.col("schedule_count") > 10, "10+_reschedules"),
    )

    # Calculate overall distribution of tasks by reschedule category
    overall_distribution = (
        task_analysis.groupBy("reschedule_category")
        .agg(F.count("*").alias("task_count"))
        .withColumn(
            "percentage",
            F.round(
                F.col("task_count")
                * 100.0
                / F.sum("task_count").over(Window.partitionBy()),
                2,
            ),
        )
        .orderBy("reschedule_category")
    )

    print("\n=== OVERALL RESCHEDULE DISTRIBUTION ===")
    overall_distribution.show()

    # Calculate first-attempt success rate (tasks that finished without reschedule)
    first_attempt_total = task_analysis.filter(F.col("schedule_count") == 1).count()
    first_attempt_success = task_analysis.filter(
        (F.col("schedule_count") == 1) & (F.col("final_event_type") == 4)
    ).count()

    print(f"\nFirst attempt tasks: {first_attempt_total}")
    print(
        f"First attempt SUCCESS (FINISH): {first_attempt_success} ({first_attempt_success*100.0/first_attempt_total:.2f}%)"
    )

    # Group tasks by priority tier for comparative analysis
    task_analysis = task_analysis.withColumn(
        "priority_group",
        F.when(F.col("priority") == 0, "free_tier")
        .when(F.col("priority") == 1, "low_priority")
        .when(F.col("priority").isin(2, 3, 5), "medium_priority")
        .when(F.col("priority") == 4, "production_tier")
        .when(F.col("priority").between(6, 11), "high_priority")
        .otherwise("unknown"),
    )

    # Create window for per-priority-group percentage calculations
    window_priority = Window.partitionBy("priority_group")

    # Calculate reschedule distribution within each priority group
    priority_reschedule = (
        task_analysis.groupBy("priority_group", "reschedule_category")
        .agg(F.count("*").alias("task_count"))
        .withColumn(
            "percentage",
            F.round(
                F.col("task_count") * 100.0 / F.sum("task_count").over(window_priority),
                2,
            ),
        )
    )

    print("\n=== RESCHEDULE DISTRIBUTION BY PRIORITY GROUP ===")
    priority_reschedule.orderBy("priority_group", "reschedule_category").show(50)

    # Calculate average reschedule statistics per priority group
    avg_by_priority = (
        task_analysis.groupBy("priority_group")
        .agg(
            F.avg("schedule_count").alias("avg_reschedules"),
            F.stddev("schedule_count").alias("stddev_reschedules"),
            F.min("schedule_count").alias("min_reschedules"),
            F.max("schedule_count").alias("max_reschedules"),
            F.count("*").alias("total_tasks"),
        )
        .orderBy("priority_group")
    )

    print("\n=== AVERAGE RESCHEDULES BY PRIORITY GROUP ===")
    avg_by_priority.show()

pass


# ============================================================================
# MAIN EXECUTION
# ============================================================================


def main():

    BASE_PATH_EDO = "/home/edoardo/Desktop/UNI/LSDMG/proj/data"
    BASE_PATH_GIU = "/home/giuse_02/Documents/Sparks/ProjectSparks/data"
    BASE_PATH_CLOUD = "gs://bucket_lsdm_dsc/data"

    spark = (
        SparkSession.builder.appName("LSDMG-Analysis")
        .master("local[4]")
        .config("spark.driver.memory", "12g")
        .config("spark.executor.memory", "12g")
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.3")
        .config("spark.sql.shuffle.partitions", "100")
        .config("spark.default.parallelism", "100")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.driver.maxResultSize", "4g")
        .getOrCreate()
    )

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    
    #Giuseppe Di Stefano path for analysis testing
    job_events = load_job_events(spark, f"{BASE_PATH_GIU}/job_events/*")
    task_events = load_task_events(spark, f"{BASE_PATH_GIU}/task_events/*")
    task_usage = load_task_usage(spark, f"{BASE_PATH_GIU}/task_usage/*")
    machine_events = load_machine_events(spark, f"{BASE_PATH_GIU}/machine_events/*")
    schema_df = load_schema(spark, f"{BASE_PATH_GIU}/schema.csv")
        

    """
    #Edoardo Cecchini path for analysis testing
    machine_events = load_machine_events(spark, f"{BASE_PATH_EDO}/machine_events/*")
    task_events = load_task_events(spark, f"{BASE_PATH_EDO}/task_events/*")
    task_usage = load_task_usage(spark, f"{BASE_PATH_EDO}/task_usage/*")
    schema_df = load_schema(spark, f"{BASE_PATH_EDO}/schema.csv")
    """

    """
    job_events = load_job_events(spark, f"{BASE_PATH_CLOUD}/job_events/*")
    task_events = load_task_events(spark, f"{BASE_PATH_CLOUD}/task_events/*")
    task_usage = load_task_usage(spark, f"{BASE_PATH_CLOUD}/task_usage/*")
    machine_events = load_machine_events(spark, f"{BASE_PATH_CLOUD}/machine_events/*")
    schema_df = load_schema(spark, f"{BASE_PATH_CLOUD}/schema.csv")
    """
    #CLOUD path for analysis
    
   
    print("#1 Analysis")
    start = timer()
    analysis_1_cpu_distribution(machine_events)
    end = timer()
    print(f"Analysis 1 completed in {end - start:.4f} seconds")

    print("\n#2 Analysis")
    start = timer()
    analysis_2_maintenance_loss(machine_events)
    print(f"Analysis 2 completed in {timer() - start:.4f} seconds")

    print("\n#3 Analysis")
    start = timer()
    analysis_3_maintenance_by_class(machine_events)
    print(f"Analysis 3 completed in {timer() - start:.4f} seconds")

    print("\n#4 Analysis")
    start = timer()
    analysis_4_jobs_tasks_distribution(job_events, task_events)
    print(f"Analysis 4 completed in {timer() - start:.4f} seconds")

    print("\n#5 Analysis")
    start = timer()
    analysis_5_killed_evicted_percentage(job_events, task_events)
    print(f"Analysis 5 completed in {timer() - start:.4f} seconds")

    print("\n#6 Analysis")
    start = timer()
    analysis_6_eviction_by_scheduling_class(task_events)
    print(f"Analysis 6 completed in {timer() - start:.4f} seconds")

    print("\n#7 Analysis")
    start = timer()
    analysis_7_task_locality(task_events)
    print(f"Analysis 7 completed in {timer() - start:.4f} seconds")


    """
     # ========== ANALYSES 8-9 ==========
    print("\n#8 Analysis")
    start = timer()
    analysis_8_resource_request_vs_consumption(task_events, task_usage)
    print(f"Analysis 8 completed in {timer() - start:.4f} seconds")

    print("\n#9 Analysis")
    start = timer()
    analysis_9_consumption_peaks_vs_eviction(machine_events, task_events, task_usage)
    print(f"Analysis 9 completed in {timer() - start:.4f} seconds")
    """

    # ========== ANALYSES 10-12  ==========
    
    print("\n#10 Analysis")
    start = timer()
    analysis_10_overcommitment_frequency(machine_events, task_events)
    print(f"Analysis 10 completed in {timer() - start:.4f} seconds")

    print("\n#11 Analysis")
    start = timer()
    analysis_11_user_task(task_events)
    print(f"Analysis 11 completed in {timer() - start:.4f} seconds")

    print("\n#12 Analysis")
    start = timer()
    analysis_12_task_reschedule_and_priority_influence(task_events)
    print(f"Analysis 12 completed in {timer() - start:.4f} seconds")

    
    spark.stop()


if __name__ == "__main__":
    main()
