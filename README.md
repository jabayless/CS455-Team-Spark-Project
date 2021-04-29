# CS455-Team-Spark-Project

## How It Works:
This project is not very user-friendly or modular (filepaths are hardcoded, etc.) but the focus of the project is to:
  - Analyze data and produce inferrences from that data
  - Utilized Apache Spark and HDFS to accomplish this project.

### Three Phases of Data Processing
#### Phase 1: Accumulating Weather Data
Running SumAndCount.class using Spark results in a dataset that accumulates the entirety of 3 years of temperature data. The resulting table includes that weather_station_id, the total of temp_min and temp_max of all daily summaries for each weather station in and around the US, and the count of those data values.

#### Phase 2: Compounding Datsets
Running Joiner.class uses Spark to read in our supplementary datasets including pop_density, household_income, cost_of_living_index, and total_population. The result is a table that includes a combination of all of these values as well as the connecting data that includes state, city, station_id, and state_id. The result is ~500 tuples of cities and stations that were included in each of the source datasets.

#### Phase 3: Creating an Index
Running Index will take the resulting dataset from phase 2 and separate the values back out into separate data structures. The standard deviation is calculated of each datafield. Then the z-score is calculated for each to get an evenly weighted value to be combined with the other zscores. This combination of z-score values is used to represent the final Happiness Score in our Index.

## Files Included

- build.gradle
- README
- /src/main/java/
  - Index.java
  - Joiner.java
  - SumAndCount.java
