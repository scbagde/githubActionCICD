# Databricks notebook source
from pyspark.sql.functions import *
import unittest,io
import datetime
from unittest.runner import TextTestResult
import sys
import json

env=dbutils.widgets.get("env")
table_name = 'product_casemanagement_ccms_aplcuserdata' if env == 'prod' else 'product_casemanagement_ccms_aplcuserdata_1_0_1'
notebook_nm = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit('/', 1)[1]

# COMMAND ----------

# MAGIC %run ./../../SOURCE/COMMON/AuditUtils

# COMMAND ----------

# MAGIC %run ./../../SOURCE/COMMON/SchedularUtils

# COMMAND ----------

# DBTITLE 1,Test Cases for aplc_user
try:
  #Extract difference between current date and edl load timestamp
  def aplc_user_date_difference():
      max_edl_ts="select from_unixtime(MAX(EDL_LOAD_TS)) as edl_ts from edl_current.{0}".format(table_name)
      df_edl_ts = sqlContext.sql(max_edl_ts)
      curr_ts =  datetime.datetime.now()
      df_edl_ts=df_edl_ts.select('edl_ts').collect()[0]
      final_edl_ts=str(df_edl_ts['edl_ts'])
      final_edl_ts=datetime.datetime.strptime(final_edl_ts, '%Y-%m-%d %H:%M:%S')
      datediff=curr_ts-final_edl_ts
      return int(datediff.days)
  
  # Testing Method Definitions
  def aplc_user_edl_query_run(query):
    df1 = spark.sql(query)
    return int(df1.select('cnt').first()[0])
      
  class aplc_user_Unit_Tests_EDL_Check_Methods(unittest.TestCase):
    
    def setUp(self):
          pass
    
    def test_aplc_user_row_count(self):
      cnt = aplc_user_edl_query_run('''
         select count(*) as cnt from edl_current.{0}
      '''.format(table_name))
      self.assertTrue(cnt > 0,'Records present in edl_current.product_casemanagement_ccms_aplcuserdata !!')
    
    def test_aplc_user_duplicate_records(self):
      cnt = aplc_user_edl_query_run('''
        select count(*) as cnt from
          (
            select CASE_ID from edl_current.{0}
            group by CASE_ID
            having count(*) > 1
          )a
      '''.format(table_name))
      self.assertTrue(cnt == 0,'Duplicates found in edl_current.product_casemanagement_ccms_aplcuserdata !!')
      
    def test_aplc_user_date_difference(self):
      cnt = aplc_user_date_difference()
      self.assertTrue(cnt <= 2 ,"EDL data ingested within last 2 days ")
      
      
  if __name__ == '__main__':
      test_classes_to_run = [
                             aplc_user_Unit_Tests_EDL_Check_Methods
                            ]
  
      suites_list = []
      test_case_result=[]
      curr_ts =  (datetime.datetime.now())
      
      for test_class in test_classes_to_run:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
        suites_list.append(suite)
        
      run_suite = unittest.TestSuite(suites_list)
      mystream = io.StringIO()
  
  #   Run the tests and store the result in mystream instead of the default sys.out
      #myTestResult = unittest.TextTestRunner(stream=mystream,verbosity=3).run(run_suite)
      myTestResult = unittest.TextTestRunner(stream=mystream,verbosity=3,resultclass=TextTestResultWithSuccesses).run(run_suite)
  #   Store the value in testoutput. This can be returned to the calling program / written to external file .
      testoutput = mystream.getvalue()
      print(testoutput)
      
      AuditUtils.extract_successful_test_cases()  
      AuditUtils.extract_failure_error_test_cases()

  AuditUtils.write_audit_results(env, 'postgresql/ccms_audit_results', test_case_result, job_nm=notebook_nm)
except Exception as error:
  error_msg = f"Error: {error}"
  print(error_msg)
  dbutils.notebook.exit(json.dumps(generate_notebook_status(notebook_nm,notebook_status="Fail", notebook_error=error_msg)))

# COMMAND ----------

dbutils.notebook.exit(json.dumps(generate_notebook_status(notebook_nm,notebook_status="Success", notebook_error=None)))