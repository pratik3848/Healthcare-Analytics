import unittest
import os, sys
from pyspark.sql import SparkSession

sys.path.insert(0, r"C:\Users\Pratik\PycharmProjects\PrescriberAnalytics\src\main\python\bin")
from presc_run_data_transform import city_report


class TransformTest(unittest.TestCase):
    def test_city_report_zip_trx_count_check(self):
        spark = SparkSession \
            .builder \
            .master('local') \
            .appName('Testing City Report Transformation') \
            .getOrCreate()

        df_city_sel = spark.read.load('city_data_riverside.csv', format='csv', header=True)
        df_fact_sel = spark.read.load('USA_Presc_Medicare_Data_2021.csv', format='csv', header=True)
        df_city_sel.show()
        df_fact_sel.show()

        df_city_final = city_report(df_city_sel, df_fact_sel)
        df_city_final.show()

        zip_cnt = df_city_final.select("zip_counts").first().zip_counts
        trx_cnt = df_city_final.select("trx_counts").first().trx_counts

        # zip_count = 14, trx_count = 45
        self.assertEqual(14, zip_cnt)


if __name__ == '__main__':
    unittest.main()
