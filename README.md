# Healthcare Prescriber Insights Project

## Overview

The Healthcare Prescriber Insights Project is a comprehensive data processing and visualization pipeline aimed at transforming raw healthcare data into actionable insights. By leveraging PySpark for data extraction, transformation, and loading, as well as AWS services for visualization, this project delivers a powerful solution to gain deeper understanding and drive informed decisions in the healthcare industry.

## Project Goals

- Provide detailed insights into healthcare prescriber data, focusing on transaction counts, city reports, and state-specific analysis.
- Automate the end-to-end data pipeline, from data extraction on GCP's HDFS to visualization on AWS Quicksight.
- Ensure data accuracy, robust exception handling, and detailed logging for smooth pipeline operation.

## Features

- **PySpark and SparkSQL Scripts:** Extract, transform, and load healthcare data from HDFS, efficiently handling both structured and semi-structured formats, while integrating seamlessly with Spark transformations.
  
- **Automated Data Pipeline:** Streamline the entire data flow process by automating data extraction, transformation, and loading with Spark on GCP's Dataproc cluster.

- **Advanced Reporting:** Generate insightful reports, including city-based transaction counts, top prescribers by state, and more.

- **AWS Integration:** Seamlessly integrate AWS S3 and Quicksight for data storage and visualization, enabling interactive and visually appealing dashboards.

- **Data Persistence:** Weekly reports are stored in HIVE for data persistence, facilitating historical analysis and trends identification.

- **Exception Handling and Logging:** Implement robust exception handling mechanisms and comprehensive logging for maintaining pipeline reliability.

- **Unit Testing:** Comprehensive suite of unit tests to validate application components and ensure data accuracy at each stage of the pipeline.

## Technologies Used

- PySpark
- SparkSQL
- HDFS
- HIVE
- GCP Dataproc
- AWS S3
- AWS Quicksight
- Unix Shell Scripting

## Project Structure

- `/bin`: Contains PySpark and SparkSQL scripts for data extraction, transformation, and loading.
- `/pipeline`: Includes Unix shell scripts for automating the end-to-end pipeline.
- `/unit_test`: Holds integration and unit tests to ensure application functionality and data accuracy.
- `/logs`: Stores detailed logs for monitoring and troubleshooting.
- `/staging`: unit tests staging data
- `/visualizations`: Placeholder for screenshots or visual representations of the generated reports.

## Screenshots
![image](https://github.com/pratik3848/Healthcare-Prescriber-Insights-Project/assets/41427089/c3717963-da55-4bee-b235-05ba3718a6a9)

![image](https://github.com/pratik3848/Healthcare-Prescriber-Insights-Project/assets/41427089/8d4d5e76-6037-4b76-a637-d67dcf57d73b)



