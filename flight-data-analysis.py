from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create a Spark session
spark = SparkSession.builder \
    .appName("Flight Data Analysis") \
    .getOrCreate()

# Load the flight data CSV into a DataFrame
flights_df = spark.read.csv("/Users/padminikota/Downloads/flights.csv", header=True, inferSchema=True)

# Task 1: Largest Discrepancy in Departure Times
def task1_largest_discrepancy(flights_df):
    # Calculate the discrepancy between scheduled and actual departure times
    top_flights_df = flights_df \
        .select("FlightNum", "CarrierCode", "Origin", "Destination", "ScheduledDeparture", "ActualDeparture", "Distance") \
        .filter(flights_df["ScheduledDeparture"].isNotNull()) \
        .withColumn("Discrepancy", F.abs(flights_df["ScheduledDeparture"] - flights_df["ActualDeparture"])) \
        .orderBy("Discrepancy", ascending=False) \
        .limit(10)

    # Show the result in the console
    top_flights_df.show()

    # Save the result to a CSV file
    top_flights_df.write.mode("overwrite").csv("output/task1_largest_discrepancy.csv", header=True)

# Task 2: Average Delay by Carrier
def task2_average_delay_by_carrier(flights_df):
    # Calculate average delay for each carrier (ScheduledDeparture - ActualDeparture)
    carrier_delay_df = flights_df \
        .select("CarrierCode", "ScheduledDeparture", "ActualDeparture") \
        .filter(flights_df["ScheduledDeparture"].isNotNull()) \
        .withColumn("Delay", flights_df["ActualDeparture"] - flights_df["ScheduledDeparture"]) \
        .groupBy("CarrierCode") \
        .agg(F.avg("Delay").alias("AvgDelay")) \
        .orderBy("AvgDelay", ascending=False)

    # Show the result in the console
    carrier_delay_df.show()

    # Save the result to a CSV file
    carrier_delay_df.write.mode("overwrite").csv("output/task2_average_delay_by_carrier.csv", header=True)

# Task 3: Total Distance Traveled by Each Origin
def task3_total_distance_by_origin(flights_df):
    # Calculate total distance traveled from each origin
    origin_distance_df = flights_df \
        .groupBy("Origin") \
        .agg(F.sum("Distance").alias("TotalDistance")) \
        .orderBy("TotalDistance", ascending=False)

    # Show the result in the console
    origin_distance_df.show()

    # Save the result to a CSV file
    origin_distance_df.write.mode("overwrite").csv("output/task3_total_distance_by_origin.csv", header=True)

# Task 4: Flight Delays by Month
def task4_flight_delays_by_month(flights_df):
    # Extract the month from the ScheduledDeparture date
    flights_df_with_month = flights_df \
        .withColumn("Month", F.month(flights_df["ScheduledDeparture"])) \
        .withColumn("Delay", flights_df["ActualDeparture"] - flights_df["ScheduledDeparture"])

    # Calculate the average delay per month
    monthly_delays_df = flights_df_with_month \
        .groupBy("Month") \
        .agg(F.avg("Delay").alias("AvgDelay")) \
        .orderBy("Month")

    # Show the result in the console
    monthly_delays_df.show()

    # Save the result to a CSV file
    monthly_delays_df.write.mode("overwrite").csv("output/task4_flight_delays_by_month.csv", header=True)

# Run all tasks
task1_largest_discrepancy(flights_df)
task2_average_delay_by_carrier(flights_df)
task3_total_distance_by_origin(flights_df)
task4_flight_delays_by_month(flights_df)

# Stop the Spark session
spark.stop()
