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
    me_cols = ["time", "missing info", "job ID", "event type", "user", "scheduling class", "job name", "logical job name"]
    df = spark.read.csv(path, header=False, inferSchema=False)
    return df.toDF(*me_cols)


def load_task_events(spark, path):
    """
    Load task events data.
    
    Args:
        spark: SparkSession object
        path: Path to task events CSV file
        
    Returns:
        DataFrame: Task events data
    """
    me_cols = ["time", "missing info", "job ID", "task index", "machine ID", "event type", "user", "scheduling class", "priority", "CPU request", "memory request", "disk space request", "different machine restrictions"]
    df = spark.read.csv(path, header=False, inferSchema=False)
    return df.toDF(*me_cols)


def load_task_usage(spark, path):
    """
    Load task usage data.
    
    Args:
        spark: SparkSession object
        path: Path to task usage CSV file
        
    Returns:
        DataFrame: Task usage data
    """
    return spark.read.csv(path, header=True, inferSchema=True)


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

# da verificare e non considerati event_type 2 (supposizione che durante il down non ci sono update)
def analysis_2_maintenance_loss(machine_events_df):
    """
    Q2: What is the percentage of computational power lost due to maintenance?
    
    Args:
        machine_events_df: Machine events DataFrame
        
    Returns:
        float: Percentage of computational power lost
    """

    w = Window.partitionBy("machine_id").orderBy("time")

    df = machine_events_df.withColumn("prev_event", F.lag("event_type", 1).over(w)) \
                            .withColumn("prev_time", F.lag("time", 1).over(w)) \
                            .withColumn("prev_cpus", F.lag("cpus", 1).over(w))
    
    df1 = df.withColumn("effective_prev_cpus", F.coalesce(F.col("prev_cpus"), F.col("cpus")))

    adds_after_remove = df1.filter(
        (F.col("event_type") == 0) &
        (F.col("prev_event") == 1) &
        (F.col("prev_time").isNotNull())
    )

    adds_with_loss = adds_after_remove.withColumn("downtime", F.col("time") - F.col("prev_time")) \
                                      .withColumn("cpu_loss", F.col("downtime") * F.col("effective_prev_cpus"))
    
    total_cpu_lost = adds_with_loss.agg(F.sum("cpu_loss").alias("total_cpu_lost")).collect()[0]["total_cpu_lost"] or 0.0

    min_t, max_t = machine_events_df.agg(F.min("time").alias("min_t"), F.max("time").alias("max_t")).first()

    obs_window = (max_t - min_t) if (min_t is not None and max_t is not None) else 0

    total_cpus = machine_events_df.select("machine_id", "cpus").dropDuplicates(["machine_id"]) \
                   .agg(F.sum("cpus").alias("sum_cpus")).collect()[0]["sum_cpus"] or 0.0

    total_possible = total_cpus * float(obs_window) if obs_window and total_cpus else 0.0

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
    
    df = df.groupBy("cpus") \
        .agg(
            F.sum(F.when((F.col("event_type") == 1) & (F.col("prev_event") == 0), 1).otherwise(0)).alias("num_down"),
            F.countDistinct("machine_id").alias("num_machines")
        ) \
        .withColumn("maintenance_rate", F.col("num_down") / F.col("num_machines"))
    
    df.show()
    
    pass


# ============================================================================
# ANALYSIS FUNCTIONS - JOBS AND TASKS
# ============================================================================

def analysis_4_jobs_tasks_distribution(job_events_df, task_events_df):
    """
    Q4: Distribution of jobs/tasks per scheduling class.
    
    Args:
        job_events_df: Job events DataFrame
        task_events_df: Task events DataFrame
        
    Returns:
        DataFrame: Distribution results
    """
    # TODO: Implement analysis
    pass


def analysis_5_killed_evicted_percentage(job_events_df, task_events_df):
    """
    Q5: Percentage of jobs/tasks that got killed or evicted.
    
    Args:
        job_events_df: Job events DataFrame
        task_events_df: Task events DataFrame
        
    Returns:
        dict: Percentages for jobs and tasks
    """
    # TODO: Implement analysis
    pass


def analysis_6_eviction_by_scheduling_class(task_events_df):
    """
    Q6: Do tasks with low scheduling class have higher eviction probability?
    
    Args:
        task_events_df: Task events DataFrame
        
    Returns:
        DataFrame: Eviction probability by scheduling class
    """
    # TODO: Implement analysis
    pass


def analysis_7_task_locality(task_events_df):
    """
    Q7: Do tasks from the same job run on the same machine?
    
    Args:
        task_events_df: Task events DataFrame
        
    Returns:
        DataFrame: Locality analysis results
    """
    # TODO: Implement analysis
    pass


# ============================================================================
# ANALYSIS FUNCTIONS - RESOURCE USAGE
# ============================================================================

def analysis_8_resource_request_vs_consumption(task_events_df, task_usage_df):
    """
    Q8: Do tasks requesting more resources consume more resources?
    
    Args:
        task_events_df: Task events DataFrame
        task_usage_df: Task usage DataFrame
        
    Returns:
        DataFrame: Correlation analysis results
    """
    # TODO: Implement analysis
    pass


def analysis_9_consumption_peaks_vs_eviction(machine_events_df, task_events_df, task_usage_df):
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


def analysis_10_overcommitment_frequency(machine_events_df, task_events_df, task_usage_df):
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

    spark = SparkSession.builder \
        .appName("LSDMG-Analysis") \
        .master("local[*]") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # Spark legge automaticamente file .gz e supporta wildcard / directory
    #job_events = load_job_events(spark, f"{BASE_PATH_GIU}/job_events/*")
    #task_events = load_task_events(spark, f"{BASE_PATH_GIU}/task_events/*")
    # task_usage = load_task_usage(spark, f"{BASE_PATH_EDO}/task_usage/*")
    
    machine_events = load_machine_events(spark, f"{BASE_PATH_EDO}/machine_events/*")
    #schema_df = load_schema(spark, f"{BASE_PATH_EDO}/schema.csv")

    # sanity checks (evita count() su dataset molto grandi in produzione)
    # print("job_events rows:", job_events.count())
    # print("task_events rows:", task_events.count())
    # print("task_usage rows:", task_usage.count())

    # cache DataFrame se lo usi spesso
    # task_events.cache()
    # task_usage.cache()

    # esempio: chiamare analisi implementate
    analysis_1_cpu_distribution(machine_events)
    analysis_2_maintenance_loss(machine_events)
    analysis_3_maintenance_by_class(machine_events)
    spark.stop()
    


if __name__ == "__main__":
    main()