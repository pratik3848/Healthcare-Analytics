import datetime as date
from pyspark.sql.functions import lit

import logging
import logging.config

logger = logging.getLogger(__name__)

def data_persist(spark, df, dfName, partitionBy, mode):
    try:
        logger.info(f"Data Persist Script - data_persist() is started for saving dataframe "+ dfName + " into Hive Table...")
        # Add a Static column with Current Date
        df= df.withColumn("delivery_date", lit(date.datetime.now().strftime("%Y-%m-%d")))
        spark.sql(""" create database if not exists prescpipeline """)
        spark.sql(""" use prescpipeline """)
        df.write.saveAsTable(dfName,partitionBy='delivery_date', mode = mode)
    except Exception as exp:
        logger.error("Error in the method - data_persist(). Please check the Stack Trace. " + str(exp),exc_info=True)
        raise
    else:
        logger.info("Data Persist - data_persist() is completed for saving dataframe "+ dfName +" into Hive Table...")

