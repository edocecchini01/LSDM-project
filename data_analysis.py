from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import desc
from pyspark import SparkContext, SparkConf

# ============================================================================
# DATA LOADING FUNCTIONS
# ============================================================================

def load_machine_events(spark, path):
    """
    Load machine events data.
    
    Args:
        spark: SparkSession object
        path: Path to machine events CSV file
        
    Returns:
        DataFrame: Machine events data
    """
    me_cols = ["time", "machine_id", "event_type", "platform_id", "cpus", "memory"]
    df = spark.read.csv(path, header=False, inferSchema=False)
    return df.toDF(*me_cols)


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
    me_cols = ["time", "missing_info", "job_ID", "event_type", "user", "scheduling_class", "job_name", "logical_job_name"]
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
    me_cols = ["time", "missing_info", "job_ID", "task_index", "machine_ID", "event_type", "user", "scheduling_class", "priority", "CPU_request", "memory_request", "disk_space_request", "different_machine_restrictions"]
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


def analysis_2_maintenance_loss(machine_events_df):
    """
    Q2: What is the percentage of computational power lost due to maintenance?
    
    Args:
        machine_events_df: Machine events DataFrame
        
    Returns:
        float: Percentage of computational power lost
    """
    # TODO: Implement analysis
    pass


def analysis_3_maintenance_by_class(machine_events_df):
    """
    Q3: Is there a class of machines with higher maintenance rate?
    
    Args:
        machine_events_df: Machine events DataFrame
        
    Returns:
        DataFrame: Maintenance rate by machine class
    """
    # TODO: Implement analysis
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
    job_events_by_scheduling_class.orderBy(desc("scheduling_class")).show()
    task_events_by_scheduling_class.orderBy(desc("scheduling_class")).show()
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
    # TODO: Implement analysis
    total_job_events = job_events.count()
    total_task_events = task_events.count()

    job_events_killed_or_evicted= job_events.filter((job_events.event_type == 5) | (job_events.event_type == 2))
    count_job = job_events_killed_or_evicted.count()
    task_events_killed_or_evicted= task_events.filter((task_events.event_type == 5) | (task_events.event_type == 2))
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
    job_events = load_job_events(spark, f"{BASE_PATH_GIU}/job_events/*")
    task_events = load_task_events(spark, f"{BASE_PATH_GIU}/task_events/*")
    # task_usage = load_task_usage(spark, f"{BASE_PATH_EDO}/task_usage/*")
    
    #machine_events = load_machine_events(spark, f"{BASE_PATH_EDO}/machine_events/*")
    #schema_df = load_schema(spark, f"{BASE_PATH_EDO}/schema.csv")

    # sanity checks (evita count() su dataset molto grandi in produzione)
    # print("job_events rows:", job_events.count())
    # print("task_events rows:", task_events.count())
    # print("task_usage rows:", task_usage.count())

    # cache DataFrame se lo usi spesso
    # task_events.cache()
    # task_usage.cache()

    # esempio: chiamare analisi implementate
    #analysis_1_cpu_distribution(machine_events)
    #print("#4 Analysis")
    #analysis_4_jobs_tasks_distribution(job_events, task_events)
    #print("#5 Analysis")
    #analysis_5_killed_evicted_percentage(job_events, task_events)


    spark.stop()
    


if __name__ == "__main__":
    main()