"""TestMetaProcessMethods"""
import os
import unittest
from datetime import datetime, timedelta

from io import StringIO, BytesIO
import boto3
import pandas as pd
from moto import mock_aws
from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess
from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import WrongMetaFileException





class TestMetaProcessMethods(unittest.TestCase):
    """
    Test the MetaProcess class
    """

    def setUp(self):
        """
        Setting up the environment
        """

        # mock the S3 connection start

        self.mock_s3 = mock_aws()
        self.mock_s3.start()
        # Defining the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'

        # Creating s3 access keys as environment variables
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'

        #creating the bucket on the mocked s3
        self.s3 = boto3.resource(service_name='s3',endpoint_url= self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                              CreateBucketConfiguration={'LocationConstraint': 'eu-central-1'})
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        # Creating a S3BucketConnector instance
        self.s3_bucket_meta = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name)
        self.dates = [(datetime.today().date() - timedelta(days=day))\
            .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(8)]

    def tearDown(self):
        # mocking s3 connection stop
        self.mock_s3.stop()


    def test_update_meta_file_no_meta_file(self):
        """
            Tests the update_meta_file when there is no meta file
        """
        #print('test_update_meta_file_no_meta_file')
        date_list_exp = ['2021-04-16','2021-04-17']
        proc_date_list_exp = [datetime.today().date()] * 2

        # Test init
        meta_key = 'meta.csv'
        # Method execution
        MetaProcess.update_meta_file(date_list_exp,meta_key,self.s3_bucket_meta)
        # Read meta file
        data = self.s3_bucket.Object(key=meta_key).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        df_meta_result = pd.read_csv(out_buffer)
        date_list_result =list(df_meta_result[MetaProcessFormat.META_SOURCE_DATE_COL.value])
        proc_date_list_result =list(
            pd.to_datetime(df_meta_result[MetaProcessFormat.META_PROCESS_COL.value]).dt.date
        )

        #Test after method execution
        self.assertEqual(date_list_exp, date_list_result)
        self.assertEqual(proc_date_list_exp,proc_date_list_result)

        #cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects':[
                    {
                        'Key': meta_key
                    }
                ]
            }
        )


    def test_update_meta_file_empty_date_list(self):
        """
            Tests the update_meta_file when theargument extract_date_list is empty
        """
        #print('test_update_meta_file_empty_date_list')
        #expected results
        return_exp = True
        #log_exp = 'The dataframe is empty! No file will be written!'
        # Test init
        date_list = []
        meta_key='meta.csv'
        # Method execution
        with self.assertLogs() as logm:
            result = MetaProcess.update_meta_file(date_list, meta_key, self.s3_bucket_meta)
            #log test after method execution
            #self.assertIn(log_exp,logm.output[1])
        #Test after method execution
        self.assertEqual(return_exp, result)
        
        #cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects':[
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_update_meta_file_meta_file_ok(self):
        """
            Tests the update_meta_file when the meta file is ok
        """
        #print('test_update_meta_file_meta_file_ok')

        date_list_old = ['2021-04-12','2021-04-13']
        date_list_new = ['2021-04-16','2021-04-17']
        date_list_exp = date_list_old + date_list_new
        proc_date_list_exp = [datetime.today().date()] * 4

        # Test init
        meta_key = 'meta.csv'
        meta_content = (
          f'{MetaProcessFormat.META_SOURCE_DATE_COL.value},'
          f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
          f'{date_list_old[0]},'
          f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n'
          f'{date_list_old[1]},'
          f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}'
        )

        self.s3_bucket.put_object(Body= meta_content,Key = meta_key)
        # Method execution
        MetaProcess.update_meta_file(date_list_new,meta_key,self.s3_bucket_meta)
        # Read meta file
        data = self.s3_bucket.Object(key=meta_key).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        df_meta_result = pd.read_csv(out_buffer)
        date_list_result =list(df_meta_result[MetaProcessFormat.META_SOURCE_DATE_COL.value])
        proc_date_list_result =list(
            pd.to_datetime(df_meta_result[MetaProcessFormat.META_PROCESS_COL.value]).dt.date
        )

        #Test after method execution
        self.assertEqual(date_list_exp, date_list_result)
        self.assertEqual(proc_date_list_exp,proc_date_list_result)

        #cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects':[
                    {
                        'Key': meta_key
                    }
                ]
            }
        )


    def test_update_meta_file_meta_file_wrong(self):
        """
            Tests the update_meta_file when the meta file is incorrect
        """

        #print('test_update_meta_file_meta_file_wrong')
        #expected results
        date_list_old = ['2021-04-12','2021-04-13']
        date_list_new = ['2021-04-16','2021-04-17']
        

        # Test init
        meta_key = 'meta.csv'
        meta_content = (
          f'wrong_column,{MetaProcessFormat.META_SOURCE_DATE_COL.value},'
          f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
          f'{date_list_old[0]},'
          f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n'
          f'{date_list_old[1]},'
          f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}'
        )

        self.s3_bucket.put_object(Body= meta_content,Key = meta_key)
        # Method execution
        with self.assertRaises(WrongMetaFileException):
             MetaProcess.update_meta_file(date_list_new,meta_key,self.s3_bucket_meta)
        
        #cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects':[
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_return_date_list_empty_date_list(self):
        """
        Tests the return_date_list method
        when there are no dates to be returned
        """

        #print('test_return_date_list_empty_date_list')
        # Expected results
        min_date_exp = '2200-01-01'
        date_list_exp = []
        # Test init
        meta_key = 'meta.csv'
        meta_content = (
          f'{MetaProcessFormat.META_SOURCE_DATE_COL.value},'
          f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
          f'{self.dates[0]},{self.dates[0]}\n'
          f'{self.dates[1]},{self.dates[0]}'
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        first_date = self.dates[0]
        # Method execution
        min_date_return, date_list_return = MetaProcess.return_date_list(first_date, meta_key,
                                                                         self.s3_bucket_meta)
        # Test after method execution
        self.assertEqual(date_list_exp, date_list_return)
        self.assertEqual(min_date_exp, min_date_return)
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

if __name__ =="__main__":
    unittest.main()