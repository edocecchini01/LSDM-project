from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark import SparkContext, SparkConf

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

    if do_cast:
        df = df.select(
            F.col("time").cast("long").alias("time"),
            F.col("machine_ID").cast("long").alias("machine_ID"),
            F.col("event_type").cast("int").alias("event_type"),
            F.col("platform_id"),
            F.col("cpus").cast("float").alias("cpus"),
            F.col("memory").cast("float").alias("memory"),
        )

    df = df.dropDuplicates()

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
    
    df = df.dropDuplicates()
    
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

    df = df.dropDuplicates()

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

    w = Window.partitionBy("machine_ID").orderBy("time")

    df = (
        machine_events_df.withColumn("prev_event", F.lag("event_type", 1).over(w))
        .withColumn("prev_time", F.lag("time", 1).over(w))
        .withColumn("prev_cpus", F.lag("cpus", 1).over(w))
    )

    df1 = df.withColumn(
        "effective_prev_cpus", F.coalesce(F.col("prev_cpus"), F.col("cpus"))
    )

    adds_after_remove = df1.filter(
        (F.col("event_type") == 0)
        & (F.col("prev_event") == 1)
        & (F.col("prev_time").isNotNull())
    )

    adds_with_loss = adds_after_remove.withColumn(
        "downtime", F.col("time") - F.col("prev_time")
    ).withColumn("cpu_loss", F.col("downtime") * F.col("effective_prev_cpus"))

    total_cpu_lost = (
        adds_with_loss.agg(F.sum("cpu_loss").alias("total_cpu_lost")).collect()[0][
            "total_cpu_lost"
        ]
        or 0.0
    )

    max_t = machine_events_df.agg(F.max("time")).first()[0]

    first_appearance = machine_events_df.groupBy("machine_ID").agg(
        F.min("time").alias("first_time"), F.first("cpus").alias("cpus")
    )

    capacity_per_machine = first_appearance.withColumn(
        "available_time", max_t - F.col("first_time")
    ).withColumn("cpu_capacity", F.col("available_time") * F.col("cpus"))

    total_possible = (
        capacity_per_machine.agg(F.sum("cpu_capacity")).collect()[0][
            "sum(cpu_capacity)"
        ]
        or 0.0
    )

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

    w = Window.partitionBy("machine_ID").orderBy("time")
    df = machine_events_df.withColumn("prev_event", F.lag("event_type", 1).over(w))

    df = (
        df.groupBy("cpus")
        .agg(
            F.sum(
                F.when(
                    (F.col("event_type") == 1) & (F.col("prev_event") == 0), 1
                ).otherwise(0)
            ).alias("num_down"),
            F.countDistinct("machine_ID").alias("num_machines"),
        )
        .withColumn("maintenance_rate", F.col("num_down") / F.col("num_machines"))
    )

    df.show()

    pass

def analysis_4_jobs_tasks_distribution(job_events, task_events):
    """
    Q4: Distribution of jobs/tasks per scheduling class.

    Args:
        job_events_df: Job events DataFrame
        task_events_df: Task events DataFrame

    Returns:
        DataFrame: Distribution results
    """
    job_events_by_scheduling_class = job_events.groupBy("scheduling_class").count()
    task_events_by_scheduling_class = task_events.groupBy("scheduling_class").count()
    job_events_by_scheduling_class.orderBy(F.desc("scheduling_class")).show()
    task_events_by_scheduling_class.orderBy(F.desc("scheduling_class")).show()
    pass


def analysis_5_killed_evicted_percentage(job_events, task_events):
    """
    Q5: Percentage of jobs/tasks that got killed or evicted.

    Args:
        job_events_df: Job events DataFrame
        task_events_df: Task events DataFrame

    Returns:
        dict: Percentages for jobs and tasks
    """
    # Aggiunta di .distinct() perchè alcuni task evited potrebbero essere ricontati perchè ritornano disponibili e cambiano il loro stato finale in submit
    # nota: aggiungendo .distinct() è stato necessario inserire anche su che parametro deve essere fatta la distizione, quindi è stato inserito anche .select()
    total_job_events = job_events.select("job_ID").distinct().count()
    total_task_events = task_events.select("job_ID", "task_index").distinct().count()

    job_events_killed_or_evicted = (
        job_events.filter((job_events.event_type == 5) | (job_events.event_type == 2))
        .select("job_ID")
        .distinct()
    )
    count_job = job_events_killed_or_evicted.count()
    task_events_killed_or_evicted = (
        task_events.filter(
            (task_events.event_type == 5) | (task_events.event_type == 2)
        )
        .select("job_ID", "task_index")
        .distinct()
    )
    count_task = task_events_killed_or_evicted.count()

    if count_job == 0:
        print("No job has been killed or evicted")
    else:
        percentage_job = (count_job / total_job_events) * 100
        print(f"Percentuale Job killed or Evicted: {percentage_job:.2f}%")

    if count_task == 0:
        print("No task has been killed or evicted")
    else:
        percentage_task = (count_task / total_task_events) * 100
        print(f"Percentuale Task Killed or Evicted: {percentage_task:.2f}%")
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

    task_requests = task_events_df.groupBy("job_ID", "task_index").agg(
        F.max("CPU_request").alias("max_cpu_request"),
        F.max("memory_request").alias("max_memory_request"),
        F.max("disk_space_request").alias("max_disk_request"),
    )

    task_consumption = task_usage_df.groupBy("job_ID", "task_index").agg(
        F.max("max_CPU_rate").alias("max_cpu_consumption"),
        F.max("max_mem_usage").alias("max_memory_consumption"),
        F.max("local_disk_space_usage").alias("max_disk_consumption"),
    )

    task_analysis = task_requests.join(
        task_consumption, on=["job_ID", "task_index"], how="inner"
    )

    print("\n--- CPU Analysis ---")

    top_cpu_requesters = (
        task_analysis.orderBy(F.desc("max_cpu_request"))
        .limit(10)
        .select("job_ID", "task_index", "max_cpu_request", "max_cpu_consumption")
    )

    print("Top 10 CPU Requesters:")
    top_cpu_requesters.show()

    top_cpu_consumers = (
        task_analysis.orderBy(F.desc("max_cpu_consumption"))
        .limit(10)
        .select("job_ID", "task_index", "max_cpu_request", "max_cpu_consumption")
    )

    print("Top 10 CPU Consumers:")
    top_cpu_consumers.show()

    print("\n--- Memory Analysis ---")

    top_memory_requesters = (
        task_analysis.orderBy(F.desc("max_memory_request"))
        .limit(10)
        .select("job_ID", "task_index", "max_memory_request", "max_memory_consumption")
    )

    print("Top 10 Memory Requesters:")
    top_memory_requesters.show()

    top_memory_consumers = (
        task_analysis.orderBy(F.desc("max_memory_consumption"))
        .limit(10)
        .select("job_ID", "task_index", "max_memory_request", "max_memory_consumption")
    )

    print("Top 10 Memory Consumers:")
    top_memory_consumers.show()

    print("\n--- Disk Analysis ---")

    top_disk_requesters = (
        task_analysis.orderBy(F.desc("max_disk_request"))
        .limit(10)
        .select("job_ID", "task_index", "max_disk_request", "max_disk_consumption")
    )

    print("Top 10 Disk Requesters:")
    top_disk_requesters.show()

    top_disk_consumers = (
        task_analysis.orderBy(F.desc("max_disk_consumption"))
        .limit(10)
        .select("job_ID", "task_index", "max_disk_request", "max_disk_consumption")
    )

    print("Top 10 Disk Consumers:")
    top_disk_consumers.show()

    pass


def analysis_9_consumption_peaks_vs_eviction(
    machine_events, task_events, task_usage
):
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
    evicted_tasks = task_events.filter(F.col("event_type") == 2) \
                               .withColumn("window_time", (F.col("time") / 300000000).cast("long") * 300000000) \
                               .select("machine_ID", "window_time") \
                               .distinct() \
                               .withColumn("has_eviction", F.lit(1))

    machine_peaks = task_usage.groupBy("machine_ID", "start_time") \
                               .agg( F.sum("max_CPU_rate").alias("total_cpu_consumption"), \
                                     F.max("max_mem_usage").alias("total_memory_consumption"), \
                                     F.max("local_disk_space_usage").alias("total_disk_consumption")) \
                               .repartition(100, "machine_ID")
    

    correlation_evicted_and_peaks = machine_peaks.join(
            evicted_tasks, \
            (machine_peaks.machine_ID == evicted_tasks.machine_ID) & (machine_peaks.start_time == evicted_tasks.window_time), \
            "left").fillna(0, subset=["has_eviction"]) \
            .persist()

    cpu_correlation = correlation_evicted_and_peaks.stat.corr("total_cpu_consumption", "has_eviction")
    mem_correlation = correlation_evicted_and_peaks.stat.corr("total_memory_consumption", "has_eviction")

    print(f"Correlation between peaks of high cpu consuption on some machines and task eviction events: {cpu_correlation}") 
    print(f"Correlation between peaks of high memory consuption on some machines and task eviction events: {mem_correlation}")
    pass


def analysis_10_overcommitment_frequency(
    machine_events_df, task_events_df
):
    """
    Q10: How often are machine resources over-committed?

    Args:
        machine_events_df: Machine events DataFrame
        task_events_df: Task events DataFrame
        task_usage_df: Task usage DataFrame

    Returns:
        DataFrame: Overcommitment frequency results
    """

    task_win = Window.partitionBy("job_ID", "task_index").orderBy("time")

    machine_win = Window.partitionBy("machine_ID").orderBy("time") \
                        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    global_machine_win = Window.partitionBy("machine_ID")

    df_deltas = task_events_df.withColumn("prev_cpu_req", F.lag("CPU_request").over(task_win)) \
                              .withColumn("prev_mem_req", F.lag("memory_request").over(task_win))

    df_deltas = df_deltas.withColumn(
        "delta_cpu",
        F.when(F.col("event_type") == 1, F.col("CPU_request"))
         .when(F.col("event_type") == 8, F.col("CPU_request") - F.col("prev_cpu_req"))
         .when(F.col("event_type").isin(2, 3, 4, 5, 6), -F.col("CPU_request"))
         .otherwise(0.0)
    ).withColumn(
        "delta_mem",
        F.when(F.col("event_type") == 1, F.col("memory_request"))
         .when(F.col("event_type") == 8, F.col("memory_request") - F.col("prev_mem_req"))
         .when(F.col("event_type").isin(2, 3, 4, 5, 6), -F.col("memory_request"))
         .otherwise(0.0)
    )

    df_raw = df_deltas.withColumn("raw_cpu", F.sum("delta_cpu").over(machine_win)) \
                      .withColumn("raw_mem", F.sum("delta_mem").over(machine_win))

    df_load = df_raw.withColumn("min_cpu", F.min("raw_cpu").over(global_machine_win)) \
                    .withColumn("min_mem", F.min("raw_mem").over(global_machine_win)) \
                    .withColumn("total_cpu_req", F.col("raw_cpu") - F.col("min_cpu")) \
                    .withColumn("total_mem_req", F.col("raw_mem") - F.col("min_mem"))

    m_events = machine_events_df.select(
        F.col("time").alias("m_time"),
        F.col("machine_ID"),
        F.col("cpus").alias("m_cpus"),
        F.col("memory").alias("m_mem")
    )

    df_joined = df_load.join(m_events, "machine_ID").filter(F.col("time") >= F.col("m_time"))

    join_win = Window.partitionBy("machine_ID", "time", "job_ID", "task_index") \
                     .orderBy(F.col("m_time").desc())

    df_with_cap = df_joined.withColumn("rank", F.row_number().over(join_win)) \
                           .filter(F.col("rank") == 1) \
                           .drop("rank", "m_time")

    df_over = df_with_cap.withColumn(
        "is_over_cpu", F.when(F.col("total_cpu_req") > F.col("m_cpus"), 1).otherwise(0)
    ).withColumn(
        "is_over_mem", F.when(F.col("total_mem_req") > F.col("m_mem"), 1).otherwise(0)
    ).withColumn(
        "is_over_gen", F.when((F.col("is_over_cpu") == 1) | (F.col("is_over_mem") == 1), 1).otherwise(0)
    )

    result = df_over.select(
        (F.mean("is_over_cpu") * 100).alias("percentage_over_cpu"),
        (F.mean("is_over_mem") * 100).alias("percentage_over_mem"),
        (F.mean("is_over_gen") * 100).alias("percentage_over_general")
    )

    result.show()

    pass


# ============================================================================
# ORIGINAL ANALYSES
# ============================================================================


def analysis_11_original_question_1():
    """
    Q11: Your original question 1.

    Motivation: [Explain the originality and relevance]

    Returns:
        DataFrame: Analysis results
    """
    # TODO: Implement original analysis
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
    
    task_events_clean = task_events.filter(F.col("time") > 0)
    
    tasks_with_lost = task_events_clean.filter(F.col("event_type") == 6) \
        .select("job_ID", "task_index").distinct()
    
    task_events_clean = task_events_clean.join(
        tasks_with_lost,
        on=["job_ID", "task_index"],
        how="left_anti"
    )
    
    schedule_counts = task_events_clean.filter(F.col("event_type") == 1) \
        .groupBy("job_ID", "task_index") \
        .agg(
            F.count("*").alias("schedule_count"),
            F.first("priority").alias("priority"),
            F.first("scheduling_class").alias("scheduling_class")
        )
    
    terminal_events = task_events_clean.filter(
        F.col("event_type").isin([3, 4, 5])
    )
    
    window_last = Window.partitionBy("job_ID", "task_index").orderBy(F.desc("time"))
    
    final_outcomes = terminal_events.withColumn("rn", F.row_number().over(window_last)) \
        .filter(F.col("rn") == 1) \
        .select("job_ID", "task_index", F.col("event_type").alias("final_event_type"))
    
    task_analysis = schedule_counts.join(
        final_outcomes,
        on=["job_ID", "task_index"],
        how="inner"
    )
    
    task_analysis = task_analysis.withColumn(
        "final_outcome",
        F.when(F.col("final_event_type") == 3, "FAIL")
        .when(F.col("final_event_type") == 4, "FINISH")
        .when(F.col("final_event_type") == 5, "KILL")
    )
    
    task_analysis.cache()
    
    task_analysis = task_analysis.withColumn(
        "reschedule_category",
        F.when(F.col("schedule_count") == 1, "first_attempt")
        .when(F.col("schedule_count") == 2, "1_reschedule")
        .when(F.col("schedule_count").between(3, 5), "2-4_reschedules")
        .when(F.col("schedule_count").between(6, 10), "5-10_reschedules")
        .when(F.col("schedule_count") > 10, "10+_reschedules")
    )
    
    overall_distribution = task_analysis.groupBy("reschedule_category") \
        .agg(F.count("*").alias("task_count")) \
        .withColumn(
            "percentage",
            F.round(F.col("task_count") * 100.0 / F.sum("task_count").over(Window.partitionBy()), 2)
        ) \
        .orderBy("reschedule_category")
    
    print("\n=== OVERALL RESCHEDULE DISTRIBUTION ===")
    overall_distribution.show()
    
    first_attempt_total = task_analysis.filter(F.col("schedule_count") == 1).count()
    first_attempt_success = task_analysis.filter(
        (F.col("schedule_count") == 1) & (F.col("final_event_type") == 4)
    ).count()
    
    print(f"\nFirst attempt tasks: {first_attempt_total}")
    print(f"First attempt SUCCESS (FINISH): {first_attempt_success} ({first_attempt_success*100.0/first_attempt_total:.2f}%)")
    
    task_analysis = task_analysis.withColumn(
        "priority_group",
        F.when(F.col("priority") == 0, "free_tier")
         .when(F.col("priority") == 1, "low_priority")
         .when(F.col("priority").isin(2, 3, 5), "medium_priority")
         .when(F.col("priority") == 4, "production_tier")
         .when(F.col("priority").between(6, 11), "high_priority")
         .otherwise("unknown")
    )
    
    window_priority = Window.partitionBy("priority_group")
    
    priority_reschedule = task_analysis.groupBy("priority_group", "reschedule_category") \
        .agg(F.count("*").alias("task_count")) \
        .withColumn(
            "percentage",
            F.round(F.col("task_count") * 100.0 / F.sum("task_count").over(window_priority), 2)
        )
    
    print("\n=== RESCHEDULE DISTRIBUTION BY PRIORITY GROUP ===")
    priority_reschedule.orderBy("priority_group", "reschedule_category").show(50)
    
    avg_by_priority = task_analysis.groupBy("priority_group") \
        .agg(
            F.avg("schedule_count").alias("avg_reschedules"),
            F.stddev("schedule_count").alias("stddev_reschedules"),
            F.min("schedule_count").alias("min_reschedules"),
            F.max("schedule_count").alias("max_reschedules"),
            F.count("*").alias("total_tasks")
        ) \
        .orderBy("priority_group")
    
    print("\n=== AVERAGE RESCHEDULES BY PRIORITY GROUP ===")
    avg_by_priority.show()
    
    pass


# ============================================================================
# MAIN EXECUTION
# ============================================================================


def main():
    
    BASE_PATH_EDO = "/home/edoardo/Desktop/UNI/LSDMG/proj/data"
    BASE_PATH_GIU = "/home/giuse_02/Documents/Sparks/ProjectSparks/data"

    spark = (
        SparkSession.builder
        .appName("LSDMG-Analysis")
        .master("local[4]")  # Limita a 4 core invece di *
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

    """
    job_events = load_job_events(spark, f"{BASE_PATH_GIU}/job_events/*")
    task_events = load_task_events(spark, f"{BASE_PATH_GIU}/task_events/*")
    task_usage = load_task_usage(spark, f"{BASE_PATH_GIU}/task_usage/*")
    machine_events = load_machine_events(spark, f"{BASE_PATH_GIU}/machine_events/*")
    schema_df = load_schema(spark, f"{BASE_PATH_GIU}/schema.csv")
    """
    
    task_events = load_task_events(spark, f"{BASE_PATH_EDO}/task_events/*")
    """
    
    task_usage = load_task_usage(spark, f"{BASE_PATH_EDO}/task_usage/*")
    machine_events = load_machine_events(spark, f"{BASE_PATH_EDO}/machine_events/*")
    schema_df = load_schema(spark, f"{BASE_PATH_EDO}/schema.csv")
    """
    

    #task_events.cache()
    #task_usage.cache()

    """
    print("#1 Analysis")
    analysis_1_cpu_distribution(machine_events)
    print("#2 Analysis")
    analysis_2_maintenance_loss(machine_events)
    print("#3 Analysis")
    analysis_3_maintenance_by_class(machine_events)
    print("#4 Analysis")
    analysis_4_jobs_tasks_distribution(job_events, task_events)
    print("#5 Analysis")
    analysis_5_killed_evicted_percentage(job_events, task_events)
    print("#6 Analysis")
    analysis_6_eviction_by_scheduling_class(task_events)
    print("#7 Analysis")
    analysis_7_task_locality(task_events)
    print("#8 Analysis")
    analysis_8_resource_request_vs_consumption(task_events, task_usage)
    print("#9 analysis")
    analysis_9_consumption_peaks_vs_eviction(machine_events, task_events, task_usage)
    print("#10 analysis")
    analysis_10_overcommitment_frequency(machine_events, task_events)
    """
    analysis_12_task_reschedule_and_priority_influence(task_events)

    spark.stop()


if __name__ == "__main__":
    main()
