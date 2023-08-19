from pyspark.sql.functions import col, dense_rank, countDistinct, sum
from pyspark.sql.window import Window
from udfs import column_split_cnt

import logging
import logging.config

logger = logging.getLogger(__name__)


def city_report(df_city_sel, df_fact_sel):
    """
    # City Report:
       Transform Logics:
       1. Calculate the Number of zips in each city.
       2. Calculate the number of distinct Prescribers assigned for each City.
       3. Calculate total TRX_CNT prescribed for each city.
       4. Do not report a city in the final report if no prescriber is assigned to it.

    Layout:
       City Name
       State Name
       County Name
       City Population
       Number of Zips
       Prescriber Counts
       Total Trx counts
    """
    try:
        logger.info(f"Transform - city_report() started.")
        df_city_split = df_city_sel.withColumn('zip_counts', column_split_cnt(df_city_sel.zips))
        df_fact_grp = df_fact_sel.groupBy(df_fact_sel.presc_state, df_fact_sel.presc_city) \
            .agg(countDistinct("presc_id").alias("presc_counts"), sum("trx_cnt").alias("trx_counts"))
        df_city_join = df_city_split.join(df_fact_grp, (df_city_split.state_name == df_fact_grp.presc_state) & (
                df_city_split.city == df_fact_grp.presc_city), 'inner')

        df_city_final = df_city_join.select("city",
                                            "state_name",
                                            "county_name",
                                            "population",
                                            "zip_counts",
                                            "trx_counts",
                                            "presc_counts")
    except Exception as exp:
        logger.error("Error in the method - city_report(). Please check the Stack Trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("Transform - city_report() is completed...")
    return df_city_final


def top_5_Prescribers(df_fact_sel, spark):
    """
    # Prescriber Report:
    Top 5 Prescribers with highest trx_cnt per each state.
    Consider the prescribers only from 20 to 50 years of experience.
    Layout:
      Prescriber ID
      Prescriber Full Name
      Prescriber State
      Prescriber Country
      Prescriber Years of Experience
      Total TRX Count
      Total Days Supply
      Total Drug Cost
    """
    try:
        logger.info("Transform - top_5_Prescribers() is started.")
        df_fact_sel.createOrReplaceTempView("presc_final")
        df_presc_final = spark.sql("""
        SELECT
            *
        FROM
        (SELECT 
            presc_state,
            presc_id,
            presc_fullname,
            SUM(trx_cnt) AS  trx_cnt,
            SUM(total_day_supply) AS total_day_supply,
            SUM(total_drug_cost) AS total_drug_cost,
            DENSE_RANK() OVER(PARTITION BY presc_state ORDER BY SUM(trx_cnt) DESC) AS rnk
        FROM 
            presc_final
        WHERE
            years_of_exp BETWEEN 20 AND 50
        GROUP BY
            presc_state,
            presc_id,
            presc_fullname) t
        WHERE
            rnk <= 5
         """)
    except Exception as exp:
        logger.error("Error in the method - top_5_Prescribers(). Please check the Stack Trace. " + str(exp),
                     exc_info=True)
        raise
    else:
        logger.info("Transform - top_5_Prescribers() is completed.")
    return df_presc_final
