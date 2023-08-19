from pyspark.sql import SparkSession
import logging
import logging.config

logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)


def get_spark_object(envn, appName):
    try:
        logger.info(f"get_spark_object() started. Current environment is: '{envn}' ")
        if envn == 'TEST':
            master = 'local'
        else:
            master = 'yarn'

        spark = SparkSession \
            .builder \
            .master(master) \
            .appName(appName) \
            .getOrCreate()
    except NameError as exp:
        logging.error("NameError in the method - get_spark_object(). Stack Trace: " + str(exp), exc_info=True)
    except Exception as exp:
        logging.error("Error in the method - get_spark_object(). Stack Trace: " + str(exp), exc_info=True)
    else:
        logging.info("Spark Object Created!")

    return spark
