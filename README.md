
# **handson-spark-Ride-Sharing Platform Analytics**

## **Prerequisites**

Before starting the assignment, ensure the following tools are installed:

1. **Python 3.x**  
   - [Download Python](https://www.python.org/downloads/)  
   - Verify installation:
     ```bash
     python --version
     ```

2. **PySpark**
   - Install using pip:
     ```bash
     pip install pyspark
     ```

3. **Faker** (for optional data simulation)
   - Install using pip:
     ```bash
     pip install faker
     ```

---

## **Setup Instructions**

- **task1.py**: Ingests and parses real-time ride data.
- **task2.py**: Aggregates total fare and average distance by driver.
- **task3.py**: Performs windowed fare trend analysis.
- **data_generator.py**: Sends ride data to a socket for testing.
- **output/**: CSV results from tasks 2 and 3.
- **checkpoint/**: Spark checkpoints for streaming recovery.

---

### **2. Running the Tasks**

Run each script in a separate terminal after starting the simulator.

```bash
python task1.py
python task2.py
python task3.py
```

---

## **Overview**

In this hands-on assignment, we build a **real-time analytics pipeline** for a ride-sharing platform using Apache Spark Structured Streaming. The system simulates real-time trip data, performs driver-level aggregations, and analyzes fare trends over time.

This assignment helps build skills in real-time streaming, JSON parsing, time-based analytics, and windowed aggregations — foundational in data engineering and stream processing.

---

## **Objectives**

1. Ingest and parse real-time JSON data from a socket.
2. Aggregate total fare and average distance by driver.
3. Perform time-based trend analysis on fare totals.
4. Output streaming results to the console and CSV.

---

## **Dataset Structure (Simulated)**

| Field        | Type    | Description                        |
|--------------|---------|------------------------------------|
| trip_id      | String  | Unique ID for each ride            |
| driver_id    | String  | ID of the driver                   |
| distance_km  | Float   | Trip distance in kilometers        |
| fare_amount  | Float   | Total fare for the trip            |
| timestamp    | String  | Ride start time (YYYY-MM-DD HH:MM:SS) |

---

## **Assignment Tasks**

---

### **1. Ingest & Parse Real-Time Ride Data**

**Objective:**
- Read streaming data from `localhost:9999`
- Parse JSON into structured fields

**Techniques Used:**
- `spark.readStream.format("socket")`
- `from_json()` with a custom schema
- `writeStream.format("console")`

**Output:**

```
+---------------------------------------+----------+-----------+-----------+-------------------+
|trip_id                                |driver_id |distance_km|fare_amount|timestamp          |
+---------------------------------------+----------+-----------+-----------+-------------------+
|3efac5bd-abac-4bbe-9cc4-ecc1346dfd40   |91        |36.24      |62.32      |2025-04-01 23:37:35|

```

---

### **2. Real-Time Aggregation by Driver**

**Objective:**
- Computing total fare and average distance grouped by driver within a time window.

**Approach:**
- Group by `driver_id` and 2-minute sliding windows
- Output to console and CSV (using `foreachBatch`)

**Output Columns:**
- `driver_id`, `total_fare`, `avg_distance`, `window_start`, `window_end`

**CSV Output:**

```
+---------+----------+------------+--------------------------+------------------------+
|driver_id|total_fare|avg_distance|    window_start          |    window_end          |
+---------+----------+------------+--------------------------+------------------------+
|85       |6.94      |20.72       |2025-04-02T00:05:00.000Z  |2025-04-02T00:07:00.000Z|
+---------+----------+------------+--------------------------+------------------------+
|35       |146.72    |12.99       |2025-04-02T00:04:00.000Z  |2025-04-02T00:06:00.000Z|
+---------+----------+------------+--------------------------+------------------------+

```

---

### **3. Windowed Fare Trend Analysis**

**Objective:**
- Analyze fare trends over time using 5-minute sliding windows (every 1 minute)

**Approach:**
- Convert string timestamps to `TimestampType`
- Use `.groupBy(window(...))` with watermark
- Output results to CSV

**Output Columns:**
- `window_start`, `window_end`, `total_fare`

**CSV Output:**

```
+------------------------+---------------------------------+---------------------+
|window_start            |window_end                       |    total_fare       |
+------------------------+---------------------------------+---------------------+
|2025-04-01T23:36:00.000Z|2025-04-01T23:41:00.000Z         |     3534.67         |
+------------------------+---------------------------------+---------------------+
```

---

## **Simulating Ride Data**

Use the provided `data_generator.py` to send sample ride data to the socket:

```bash
python data_generator.py
```

The simulator generates one ride record per second in this format:

```json
{
  "trip_id": "t001",
  "driver_id": "d1",
  "distance_km": 5.2,
  "fare_amount": 12.5,
  "timestamp": "2025-04-02 00:00:00"
}
```

---


## **Conclusion**

This assignment gives hands-on experience in real-time streaming analytics, simulating real-world ride data. It prepares for data engineering roles working with tools like Apache Spark, Kafka, and streaming data pipelines.

---
