from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    # Create Spark session
    spark = SparkSession.builder.appName("DataFrameExample").getOrCreate()

    # Create a DataFrame inline
    data = [
        ("John", 28),
        ("Jane", 34),
        ("Jane", 1134),
        ("Doe", 23),
        ("John", 29)
    ]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)

    # Perform some transformations: group by name and calculate average age
    avg_age_df = df.groupBy("Name").avg("Age").withColumnRenamed("avg(Age)", "AverageAge")

    # Display the output
    avg_age_df.show()

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()