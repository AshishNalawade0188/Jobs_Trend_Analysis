#!/usr/bin/env python3
"""Job Data Cleaning Pipeline — PySpark (CLI-ready)."""

import os
import sys
import argparse
import subprocess

# Fix: a locally installed Spark (SPARK_HOME/PYTHONPATH) can conflict with
# pip's pyspark and its bundled py4j, causing
# "py4j.protocol.Py4JError: ... does not exist in the JVM". Clear them so
# pip's pyspark uses its own matching jars.
os.environ.pop("SPARK_HOME", None)
os.environ.pop("PYTHONPATH", None)


def ensure_pyspark_installed():
    try:
        import pyspark  # noqa: F401
        print(f" PySpark {pyspark.__version__} already installed")
    except ImportError:
        print("Installing PySpark ...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyspark", "--quiet"])
        print(" PySpark installed")


BAD_VALUES = [
    "",
    "--", "-", "*", "---", "@@@", "#####","0","[NULL]","999"," ","  ","   ",
    "invalid", "!!INVALID!!",
    "#VALUE!", "#N/A", "#NAME?", "#VALUE","#ERROR!",
    "unknown", "Unknown", "UNKNOWN",
    "???",
    "#REF!", "#ERROR",
    "n.a", "N.A", "n.a.", "N.A.","[N/A]",
    "na", "NA",
    "N/A", "n/a",
    "TBD", "tbd",
    "xxx", "XXX", "XXXX",
    "<NULL>", "NULL", "null", "Null",
    "9999",
    "-1",
    "none", "None", "NONE",
    "NaN", "nan", "NAN",
    "Infinity", "-Infinity", "infinity", "-infinity",
    "Inf", "-Inf", "inf", "-inf",
]

NUMERIC_COLS = [
    "Salary_Min", "Salary_Max", "Number_of_Openings",
    "Applicants_Count", "Interview_Rounds", "Selection_Rate",
]

DATE_COLS = ["Posted_Date", "Application_Deadline"]

OUTLIER_COLS = NUMERIC_COLS + ["Avg_Salary"]


def create_spark_session():
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("Job_Data_Cleaning_Pipeline")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print(f" Spark session created | version: {spark.version}")
    return spark


def load_data(spark, csv_path):
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(csv_path)
    )
    print(f" Dataset loaded | Rows: {df.count():,} | Columns: {len(df.columns)}")
    return df


def preview_data(df):
    df.show(5, truncate=60)
    df.printSchema()
    return df


def trim_whitespace(df):
    from pyspark.sql.functions import col, trim
    for c in df.columns:
        df = df.withColumn(c, trim(col(c)))
    print(" Whitespace trimmed")
    return df


def replace_bad_values(df):
    from pyspark.sql.functions import col, when
    for c in df.columns:
        df = df.withColumn(c, when(col(c).isin(BAD_VALUES), None).otherwise(col(c)))
    print(f" {len(BAD_VALUES)} dirty sentinel values replaced with NULL")
    return df


def print_null_summary(df):
    from pyspark.sql.functions import col
    null_counts = [(c, df.filter(col(c).isNull()).count()) for c in df.columns]
    null_counts.sort(key=lambda x: -x[1])
    print(f"\n{'Column':<30} {'Null Count':>12}")
    print("-" * 44)
    for col_name, cnt in null_counts:
        flag = " ⚠️" if cnt > 0 else ""
        print(f"{col_name:<30} {cnt:>12,}{flag}")


def cast_numeric_columns(df):
    from pyspark.sql.functions import col
    from pyspark.sql.types import DoubleType
    for c in NUMERIC_COLS:
        df = df.withColumn(c, col(c).cast(DoubleType()))
    print(" Numeric columns cast to Double")
    return df


def nullify_nan_values(df):
    from pyspark.sql.functions import col, when, isnan
    for c in NUMERIC_COLS:
        df = df.withColumn(
            c,
            when(isnan(col(c)) | (col(c) == float("inf")) | (col(c) == float("-inf")), None)
            .otherwise(col(c))
        )
    print(" Residual NaN / Infinity values nulled out")
    return df


def nullify_negative_values(df):
    from pyspark.sql.functions import col, when
    for c in NUMERIC_COLS:
        df = df.withColumn(c, when(col(c) < 0, None).otherwise(col(c)))
    print(" Negative values replaced with NULL")
    return df


def impute_numeric(df):
    """Mean fill by default; if a column has literal 0 values, null them
    out and fill with the median instead."""
    from pyspark.sql.functions import avg, col, when
    for c in NUMERIC_COLS:
        zero_count = df.filter(col(c) == 0).count()
        if zero_count > 0:
            df = df.withColumn(c, when(col(c) == 0, None).otherwise(col(c)))
            median_result = df.approxQuantile(c, [0.5], 0.01)
            median_val = median_result[0] if median_result else None
            if median_val is not None:
                df = df.fillna(median_val, subset=[c])
                print(f"   {c:<25} {zero_count} zero(s) -> filled with median = {median_val:.4f}")
            else:
                print(f"   {c:<25} ⚠️  entire column null after removing zeros — skipped")
        else:
            mean_val = df.select(avg(c)).first()[0]
            if mean_val is not None:
                df = df.fillna(mean_val, subset=[c])
                print(f"   {c:<25} filled with mean = {mean_val:.4f}")
            else:
                print(f"   {c:<25} ⚠️  entire column is null — skipped")
    print(" Numeric null imputation complete")
    return df


def get_categorical_cols(df):
    return [c for c in df.columns if c not in NUMERIC_COLS and c not in DATE_COLS]


def impute_categorical_mode(df, categorical_cols):
    from pyspark.sql.functions import col, desc
    for c in categorical_cols:
        mode_row = (
            df.filter(col(c).isNotNull())
            .groupBy(c).count()
            .orderBy(desc("count"))
            .first()
        )
        if mode_row is None:
            df = df.fillna("Unknown", subset=[c])
            print(f"   {c:<30} ⚠️  all null — filled with 'Unknown'")
        else:
            df = df.fillna(mode_row[0], subset=[c])
            print(f"   {c:<30} mode = '{mode_row[0]}'")
    print(" Categorical null imputation complete")
    return df


def clean_date_columns(df):
    """Parses dates and guarantees Posted_Date <= today, Application_Deadline
    <= today, and 0 < gap <= 60 days. Invalid/missing dates are regenerated."""
    from pyspark.sql.functions import col, when, to_date, current_date, rand, datediff, expr, floor

    df = df.withColumn("Posted_Date", to_date(col("Posted_Date"), "M/d/yyyy"))
    df = df.withColumn("Application_Deadline", to_date(col("Application_Deadline"), "M/d/yyyy"))

    df = df.withColumn("_rand_days_back", (floor(rand(seed=42) * 670) + 60).cast("int"))
    df = df.withColumn("_rand_gap", (floor(rand(seed=99) * 60) + 1).cast("int"))

    df = df.withColumn(
        "Posted_Date",
        when(col("Posted_Date").isNull(), expr("date_sub(current_date(), _rand_days_back)"))
        .otherwise(col("Posted_Date"))
    )

    safe_max_posted = expr("date_sub(current_date(), 60)")
    df = df.withColumn(
        "Posted_Date",
        when(col("Posted_Date") > safe_max_posted, safe_max_posted).otherwise(col("Posted_Date"))
    )

    generated_deadline = expr("date_add(Posted_Date, _rand_gap)")
    gap_days = datediff(col("Application_Deadline"), col("Posted_Date"))

    df = df.withColumn(
        "Application_Deadline",
        when(
            col("Application_Deadline").isNull()
            | (gap_days <= 0)
            | (gap_days > 60)
            | (col("Application_Deadline") > current_date()),
            generated_deadline
        ).otherwise(col("Application_Deadline"))
    )

    df = df.drop("_rand_days_back", "_rand_gap")
    print(" Date columns cleaned")
    return df


def filter_old_rows(df, cutoff="2010-01-01"):
    from pyspark.sql.functions import col, to_date, lit
    cutoff_date = to_date(lit(cutoff), "yyyy-MM-dd")
    before = df.count()
    df = df.filter(col("Posted_Date") >= cutoff_date)
    print(f" Old rows removed | {before:,} -> {df.count():,}")
    return df


def engineer_avg_salary(df):
    from pyspark.sql.functions import col
    df = df.withColumn("Avg_Salary", (col("Salary_Min") + col("Salary_Max")) / 2)
    print(" 'Avg_Salary' feature engineered")
    return df


def round_selection_rate(df):
    from pyspark.sql.functions import col, round as spark_round
    df = df.withColumn("Selection_Rate", spark_round(col("Selection_Rate"), 2))
    print(" Selection_Rate rounded")
    return df


def cap_outliers_iqr(df):
    from pyspark.sql.functions import col, when

    df.select(OUTLIER_COLS).summary().show()

    bounds = {}
    for c in OUTLIER_COLS:
        q1, q3 = df.approxQuantile(c, [0.25, 0.75], 0.01)
        iqr = q3 - q1
        lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        bounds[c] = (lower, upper)
        print(f"  {c:<25} Bounds=[{lower:.2f}, {upper:.2f}]")

    for c in OUTLIER_COLS:
        lower, upper = bounds[c]
        df = df.withColumn(
            c, when(col(c) < lower, lower).when(col(c) > upper, upper).otherwise(col(c))
        )

    print(" Outliers capped")
    df.select(OUTLIER_COLS).summary().show()
    return df


def replace_zeros_with_median_final(df):
    """Final safety-net pass: any residual 0 values in the numeric/outlier
    columns (including the engineered Avg_Salary) are nulled out and
    filled with that column's median. This catches zeros that could have
    reappeared after casting, outlier capping, or feature engineering."""
    from pyspark.sql.functions import col, when

    print("\n Final zero -> median replacement pass:")
    for c in OUTLIER_COLS:
        zero_count = df.filter(col(c) == 0).count()
        if zero_count > 0:
            df = df.withColumn(c, when(col(c) == 0, None).otherwise(col(c)))
            median_result = df.approxQuantile(c, [0.5], 0.01)
            median_val = median_result[0] if median_result else None
            if median_val is not None:
                df = df.fillna(median_val, subset=[c])
                print(f"   {c:<25} {zero_count} zero(s) -> filled with median = {median_val:.4f}")
            else:
                print(f"   {c:<25} ⚠️  entire column null after removing zeros — skipped")
        else:
            print(f"   {c:<25} no zeros found")
    print(" Final zero replacement complete")
    return df


def drop_duplicates(df):
    before = df.count()
    df = df.dropDuplicates()
    print(f" Duplicates removed: {before - df.count():,}")
    return df


def save_clean_csv(df, output_folder, filename):
    """
    Convert to pandas and write to disk.

    FIX: this Python 3.6 environment's pandas has a bug in
    get_filepath_or_buffer() where writing to a brand-new file via a plain
    path STRING can fall through into urllib's urlopen(), which tries to
    os.stat() the file as if reading it -> FileNotFoundError wrapped in a
    URLError, even though the folder exists. Passing an already-open file
    HANDLE instead of a path string skips that buggy code path entirely.

    output_folder may be a plain path or a "file:///..." URI (Spark-style);
    both are normalized to a plain local path before writing with pandas.
    """
    # Normalize a file:// URI (used for Spark) down to a plain local path
    # for the pandas/os steps below.
    local_output_folder = output_folder
    if local_output_folder.startswith("file://"):
        local_output_folder = local_output_folder[len("file://"):]

    # 1. Make sure the output directory actually exists
    os.makedirs(local_output_folder, exist_ok=True)
    if not os.path.isdir(local_output_folder):
        raise RuntimeError(f"Failed to create output folder: {local_output_folder}")

    output_path = os.path.join(local_output_folder, filename)

    # 2. Convert to pandas
    pdf = df.toPandas()

    # 3. Write via an explicit file handle (avoids the pandas path/URL bug)
    try:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            pdf.to_csv(f, index=False)
    except OSError as e:
        raise RuntimeError(
            f"Could not write CSV to '{output_path}'. "
            f"Check that the path is correct and that you have write "
            f"permission to '{local_output_folder}'."
        ) from e

    print(f"✅ Clean dataset saved to: {output_path}")
    return output_path


def run_pipeline(input_csv, output_folder, output_filename):
    ensure_pyspark_installed()
    spark = create_spark_session()

    df = load_data(spark, input_csv)
    df = preview_data(df)
    df = trim_whitespace(df)
    df = replace_bad_values(df)
    print_null_summary(df)

    df = cast_numeric_columns(df)
    df = nullify_nan_values(df)
    df = nullify_negative_values(df)
    df = impute_numeric(df)

    categorical_cols = get_categorical_cols(df)
    df = impute_categorical_mode(df, categorical_cols)

    df = clean_date_columns(df)
    df = filter_old_rows(df)

    df = engineer_avg_salary(df)
    df = round_selection_rate(df)
    df = cap_outliers_iqr(df)
    df = replace_zeros_with_median_final(df)
    df = drop_duplicates(df)

    if "Location_Country" in df.columns:
        df = df.drop("Location_Country")
        print(" 'Location_Country' column dropped")

    df.printSchema()
    df.show(10, truncate=50)
    print(f" Final dataset | Rows: {df.count():,} | Columns: {len(df.columns)}")

    output_path = save_clean_csv(df, output_folder, output_filename)
    spark.stop()
    return output_path


def parse_args():
    parser = argparse.ArgumentParser(description="Job Data Cleaning Pipeline (PySpark)")
    parser.add_argument("--input", default="file:///home/talentum/test-jupyter/Job_Market_Analytics/data/Job_Analytics.csv")
    parser.add_argument("--output", default="file:///home/talentum/test-jupyter/Job_Market_Analytics/Cleaned_data")
    parser.add_argument("--filename", default="Job_Analytics_Clean.csv")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(args.input, args.output, args.filename)
