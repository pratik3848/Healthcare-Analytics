import sys
from presc_run_data_extraction import extract_files
from create_objects import get_spark_object
import get_all_variables as gav
from validations import get_curr_date, df_count, df_top10_rec, df_print_schema
from presc_run_data_preprocessing import perform_data_clean
from presc_run_data_transform import city_report, top_5_Prescribers
import logging
import logging.config
import os
from presc_run_data_ingest import load_files
from subprocess import Popen, PIPE
logging.config.fileConfig(fname='../util/logging_to_file.conf')
from presc_run_data_persist import data_persist

def main():
    try:
        logging.info("main() method started ...")
        ### Get Spark Object
        spark = get_spark_object(gav.envn, gav.appName)
        # Validate Spark Object
        get_curr_date(spark)

        ### Initiate run_presc_data_ingest
        # Load City Dimension File
        file_dir="PrescPipeline/staging/dimension_city"
        proc = Popen(['hdfs', 'dfs', '-ls', '-C', file_dir], stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()
        if 'parquet' in out.decode():
           file_format = 'parquet'
           header='NA'
           inferSchema='NA'
        elif 'csv' in out.decode():
           file_format = 'csv'
           header=gav.header
           inferSchema=gav.inferSchema

        df_city = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                             inferSchema=inferSchema)

        # Load the Prescriber Fact File
        file_dir="PrescPipeline/staging/fact"
        proc = Popen(['hdfs', 'dfs', '-ls', '-C', file_dir], stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()
        if 'parquet' in out.decode():
           file_format = 'parquet'
           header='NA'
           inferSchema='NA'
        elif 'csv' in out.decode():
           file_format = 'csv'
           header=gav.header
           inferSchema=gav.inferSchema

        df_fact = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                             inferSchema=inferSchema)

        # Validate city dimension
        # df_count(df_city, 'df_city')
        # df_top10_rec(df_city, 'df_city')

        # validate fact file
        # df_count(df_fact, 'df_fact')
        # df_top10_rec(df_fact, 'df_fact')

        ### Initiate run_presc_data_preprocessing script
        # Perform data cleaning operations on city_df
        cleaned_city_df, cleaned_df_fact = perform_data_clean(df_city, df_fact)

        # Validate cleaned_city_df
        # df_top10_rec(cleaned_city_df, 'cleaned_city_df')

        # Validate cleaned_df_fact
        # df_top10_rec(cleaned_df_fact, 'cleaned_df_fact')
        # df_print_schema(cleaned_df_fact, 'cleaned_df_fact')

        ### Initiate run_presc_data_transform script
        city_report_final = city_report(cleaned_city_df, cleaned_df_fact)
        # df_top10_rec(city_report_final, 'city_report_final')
        # Apply all the transformation Logics
        df_presc_final = top_5_Prescribers(cleaned_df_fact, spark=spark)
        df_top10_rec(df_presc_final, 'df_presc_final')
        # Validate
        # Set up logging
        # Set up Error Handling
        ### Initiate run_data_extraction Script
        CITY_PATH=gav.output_city
        extract_files(city_report_final,'json',CITY_PATH,1,False,'none')
     
        PRESC_PATH=gav.output_fact
        extract_files(df_presc_final,'orc',PRESC_PATH,2,False,'none')
        
        data_persist(spark = spark, df = city_report_final ,dfName = 'df_city_final' ,partitionBy = 'delivery_date',mode='append')
        data_persist(spark = spark, df = df_presc_final,dfName = 'df_presc_final',partitionBy = 'delivery_date',mode='append')


        logging.info("presc_run_pipeline.py completed!")
    except Exception as exp:
        logging.error("Error occured in the main() mehtod. Check stack trace at the respective module!" + str(exp),
                      exc_info=True)
        sys.exit(1)

    ### Initiate run_data_extraction script
    # Validate
    # Set up logging
    # Set up Error Handling


if __name__ == '__main__':
    logging.info("Prescriber application started ...")
    main()
