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
    me_cols = ["time", "machine_id", "event_type", "platform_id", "cpus", "memory"]
    df = spark.read.csv(path, header=False, inferSchema=False).toDF(*me_cols)

    if do_cast:
        df = df.select(
            F.col("time").cast("long").alias("time"),
            F.col("machine_id").cast("long").alias("machine_id"),
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
    
    #df = df.dropDuplicates()
    
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
            F.col("local_disk_space_usage").cast("float").alias("local_disk_space_usage"),
            F.col("max_CPU_rate").cast("float").alias("max_CPU_rate"),
            F.col("max_io_time").cast("float").alias("max_io_time"),
            F.col("cycles_per_instruction").cast("float").alias("cycles_per_instruction"),
            F.col("mem_accesses_per_instruction").cast("float").alias("mem_accesses_per_instruction"),
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


# ============================================================================
# ANALYSIS FUNCTIONS - MACHINES
# ============================================================================


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


# not take in account the change of the machine (event_type = 2)
def analysis_2_maintenance_loss(machine_events_df):
    """
    Q2: What is the percentage of computational power lost due to maintenance?

    Args:
        machine_events_df: Machine events DataFrame

    Returns:
        float: Percentage of computational power lost
    """

    w = Window.partitionBy("machine_id").orderBy("time")

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

    first_appearance = machine_events_df.groupBy("machine_id").agg(
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

    w = Window.partitionBy("machine_id").orderBy("time")
    df = machine_events_df.withColumn("prev_event", F.lag("event_type", 1).over(w))

    df = (
        df.groupBy("cpus")
        .agg(
            F.sum(
                F.when(
                    (F.col("event_type") == 1) & (F.col("prev_event") == 0), 1
                ).otherwise(0)
            ).alias("num_down"),
            F.countDistinct("machine_id").alias("num_machines"),
        )
        .withColumn("maintenance_rate", F.col("num_down") / F.col("num_machines"))
    )

    df.show()

    pass


# ============================================================================
# ANALYSIS FUNCTIONS - JOBS AND TASKS
# ============================================================================


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
    job_events = job_events.withColumn("job_id", F.col("job_id").cast("long"))
    task_events = task_events.withColumn("job_id", F.col("job_id").cast("long"))
    # Aggiunta di .distinct() perchè alcuni task evited potrebbero essere ricontati perchè ritornano disponibili e cambiano il loro stato finale in submit
    # nota: aggiungendo .distinct() è stato necessario inserire anche su che parametro deve essere fatta la distizione, quindi è stato inserito anche .select()
    total_job_events = job_events.select("job_id").distinct().count()
    total_task_events = task_events.select("job_id", "task_index").distinct().count()

    job_events_killed_or_evicted = (
        job_events.filter((job_events.event_type == 5) | (job_events.event_type == 2))
        .select("job_id")
        .distinct()
    )
    count_job = job_events_killed_or_evicted.count()
    task_events_killed_or_evicted = (
        task_events.filter(
            (task_events.event_type == 5) | (task_events.event_type == 2)
        )
        .select("job_id", "task_index")
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
    task_events = task_events.withColumn("job_id", F.col("job_id").cast("long"))
    task_events_low_scheduling_class = task_events.filter(
        task_events.scheduling_class < 3
    )
    total_events_low_scheduling_class = (
        task_events_low_scheduling_class.select("job_id", "task_index")
        .distinct()
        .count()
    )
    task_events_high_scheduling_class = task_events.filter(
        task_events.scheduling_class == 3
    )
    total_events_high_scheduling_class = (
        task_events_high_scheduling_class.select("job_id", "task_index")
        .distinct()
        .count()
    )

    count_evicted_low = (
        task_events_low_scheduling_class.filter(
            task_events_low_scheduling_class.event_type == 2
        )
        .select("job_id", "task_index")
        .distinct()
        .count()
    )
    count_evicted_high = (
        task_events_high_scheduling_class.filter(
            task_events_high_scheduling_class.event_type == 2
        )
        .select("job_id", "task_index")
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
    task_events = task_events.withColumn("job_id", F.col("job_id").cast("long"))
    task_events_same_jobs = task_events.groupBy("job_id").agg(
        F.count(task_events.task_index).alias("total_task_per_job"),
        F.count_distinct(task_events.machine_ID).alias("distinct_machines_per_task"),
    )

    locality_distribution = task_events_same_jobs.select(
        "*",
        (F.col("distinct_machines_per_task") / F.col("total_task_per_job") * 100).alias(
            "locality_distribution"
        ),
    )
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
    
    task_requests = task_events_df.groupBy(
        "job_ID", "task_index"
    ).agg(
        F.max("CPU_request").alias("max_cpu_request"),
        F.max("memory_request").alias("max_memory_request"),
        F.max("disk_space_request").alias("max_disk_request")
    )
    
    task_consumption = task_usage_df.groupBy(
        "job_ID", "task_index"
    ).agg(
        F.max("max_CPU_rate").alias("max_cpu_consumption"),
        F.max("max_mem_usage").alias("max_memory_consumption"),
        F.max("local_disk_space_usage").alias("max_disk_consumption")
    )

    task_analysis = task_requests.join(
        task_consumption,
        on=["job_ID", "task_index"],
        how="inner"
    )
    
    print("\n--- CPU Analysis ---")
    
    top_cpu_requesters = task_analysis.orderBy(
        F.desc("max_cpu_request")
    ).limit(10).select(
        "job_ID", "task_index", "max_cpu_request", "max_cpu_consumption"
    )
    
    print("Top 10 CPU Requesters:")
    top_cpu_requesters.show()
    
    top_cpu_consumers = task_analysis.orderBy(
        F.desc("max_cpu_consumption")
    ).limit(10).select(
        "job_ID", "task_index", "max_cpu_request", "max_cpu_consumption"
    )
    
    print("Top 10 CPU Consumers:")
    top_cpu_consumers.show()
    
    print("\n--- Memory Analysis ---")
    
    top_memory_requesters = task_analysis.orderBy(
        F.desc("max_memory_request")
    ).limit(10).select(
        "job_ID", "task_index", "max_memory_request", "max_memory_consumption"
    )
    
    print("Top 10 Memory Requesters:")
    top_memory_requesters.show()
    
    top_memory_consumers = task_analysis.orderBy(
        F.desc("max_memory_consumption")
    ).limit(10).select(
        "job_ID", "task_index", "max_memory_request", "max_memory_consumption"
    )
    
    print("Top 10 Memory Consumers:")
    top_memory_consumers.show()
    
    print("\n--- Disk Analysis ---")
    
    top_disk_requesters = task_analysis.orderBy(
        F.desc("max_disk_request")
    ).limit(10).select(
        "job_ID", "task_index", "max_disk_request", "max_disk_consumption"
    )
    
    print("Top 10 Disk Requesters:")
    top_disk_requesters.show()
    
    top_disk_consumers = task_analysis.orderBy(
        F.desc("max_disk_consumption")
    ).limit(10).select(
        "job_ID", "task_index", "max_disk_request", "max_disk_consumption"
    )
    
    print("Top 10 Disk Consumers:")
    top_disk_consumers.show()

    pass


def analysis_9_consumption_peaks_vs_eviction(
    machine_events_df, task_events_df, task_usage_df
):
    """
    Q9: Correlation between resource consumption peaks and task evictions.

    Args:
        machine_events_df: Machine events DataFrame
        task_events_df: Task events DataFrame
        task_usage_df: Task usage DataFrame

    Returns:
        DataFrame: Correlation results
    """
    # TODO: Implement analysis
    pass


def analysis_10_overcommitment_frequency(
    machine_events_df, task_events_df, task_usage_df
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
    # TODO: Implement analysis
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


def analysis_12_original_question_2():
    """
    Q12: Your original question 2.

    Motivation: [Explain the originality and relevance]

    Returns:
        DataFrame: Analysis results
    """
    # TODO: Implement original analysis
    pass


# ============================================================================
# MAIN EXECUTION
# ============================================================================


def main():
    """
    Main execution function.
    Execute all analyses and save results.
    """

    BASE_PATH_EDO = "/home/edoardo/Desktop/UNI/LSDMG/proj/data"
    BASE_PATH_GIU = "/home/giuse_02/Documents/Sparks/ProjectSparks/data"

    spark = (
        SparkSession.builder.appName("LSDMG-Analysis").master("local[*]").getOrCreate()
    )

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # Spark legge automaticamente file .gz e supporta wildcard / directory
    """
    job_events = load_job_events(spark, f"{BASE_PATH_GIU}/job_events/*")
    task_events = load_task_events(spark, f"{BASE_PATH_GIU}/task_events/*")
    """
    #task_events = load_task_events(spark, f"{BASE_PATH_EDO}/task_events/*")
    #task_usage = load_task_usage(spark, f"{BASE_PATH_EDO}/task_usage/*")

    # machine_events = load_machine_events(spark, f"{BASE_PATH_EDO}/machine_events/*")
    # schema_df = load_schema(spark, f"{BASE_PATH_EDO}/schema.csv")

    # sanity checks (evita count() su dataset molto grandi in produzione)
    # print("job_events rows:", job_events.count())
    # print("task_events rows:", task_events.count())
    # print("task_usage rows:", task_usage.count())

    # cache DataFrame se lo usi spesso
    # task_events.cache()
    # task_usage.cache()

    # esempio: chiamare analisi implementate
    # analysis_1_cpu_distribution(machine_events)
    # analysis_2_maintenance_loss(machine_events)
    # analysis_3_maintenance_by_class(machine_events)
    #analysis_8_resource_request_vs_consumption(task_events, task_usage)

    """
     print("#4 Analysis")
    analysis_4_jobs_tasks_distribution(job_events, task_events)
    print("#5 Analysis")
    analysis_5_killed_evicted_percentage(job_events, task_events)
    print("#6 Analysis")
    analysis_6_eviction_by_scheduling_class(task_events)
    print("#7 Analysis")
    analysis_7_task_locality(task_events)
    """

    spark.stop()


if __name__ == "__main__":
    main()
