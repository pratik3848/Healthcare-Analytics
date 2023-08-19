import logging
import logging.config
import pandas
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)


def get_curr_date(spark):
    try:
        opDF = spark.sql(""" SELECT CURRENT_DATE""")
        logger.info("Validate the Spark Object -" + str(opDF.collect()))
    except NameError as exp:
        logger.error("NameError in the method - spark_curr_date(). Stack Trace: " + str(exp), exc_info=True)
        raise
    except Exception as exp:
        logger.error("Error in the method - spark_curr_date(). Stack Trace: " + str(exp), exc_info=True)
    else:
        logger.info("Spark Object Validated. Spark Object is ready.")


def df_count(df, dfName):
    try:
        logger.info(f"The DataFrame Validation by count df_count() is started for Dataframe: '{dfName}'")
        df_count = df.count()
        logger.info(f"The DataFrame count is {df_count}.")
    except Exception as exp:
        logger.error("Error in the method - df_count(). Please check the Stack Trace. " + str(exp))
        raise
    else:
        logger.info(f"The DataFrame Validation by count df_count() is completed.")


def df_top10_rec(df, dfName):
    try:
        logger.info(f"The DataFrame Validation by top 10 record df_top10_rec() is started for Dataframe: '{dfName}'")
        logger.info(f"The DataFrame top 10 records are:")
        df_pandas = df.limit(10).toPandas()
        logger.info('\n \t' + df_pandas.to_string(index=False))
    except Exception as exp:
        logger.error("Error in the method - df_top10_rec(). Please check the Stack Trace. " + str(exp))
        raise
    else:
        logger.info("The DataFrame Validation by top 10 record df_top10_rec() is completed.")


def df_print_schema(df, dfName):
    try:
        logger.info(f"Dataframe schema validation for '{dfName}'")
        sch = df.schema.fields
        logger.info(f"Schema for '{dfName}': ")
        for i in sch:
            logger.info(f"\t{i}")
    except Exception as exp:
        logger.error("Error in the method - df_print_schema(). Please check the Stack Trace. " + str(exp))
        raise
    else:
        logger.info("DataFrame Schema Validation completed!")