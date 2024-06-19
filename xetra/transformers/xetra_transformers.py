"""Xetra ETL Component"""
import pandas as pd

import logging
from typing import NamedTuple
from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess


class XetraSourceConfig(NamedTuple):
    """
    Class for source configuration data
    
    """

    src_first_extract_date: str
    src_columns: str
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_vol: str


class XetraTargetConfig(NamedTuple):
    """
    Class for target configuration data


    """
    trg_col_date: str
    trg_col_isin: str
    trg_col_op_price: str
    trg_col_clos_price: str
    trg_col_min_price: str
    trg_col_max_price: str
    trg_col_dail_trad_vol: str
    trg_col_ch_prev_clos: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str


class XetraETL():
    """
    Reads the Xetra data, transforms and writes the transformed data to target
    """

    def __init__(self, s3_bucket_Src: S3BucketConnector,
                 s3_bucket_trg: S3BucketConnector,meta_key: str,
                 src_args: XetraSourceConfig, trg_args: XetraTargetConfig):
        
        """
        Contructor for XetraTransformer
        """
        self._logger = logging.getLogger(__name__)
        self.s3_bucket_src = s3_bucket_Src
        self.s3_bucket_trg = s3_bucket_trg
        self.meta_key = meta_key
        self.src_args = src_args
        self.trg_args = trg_args
        self.extract_date, self.extract_date_list = MetaProcess.return_date_list(
            self.src_args.src_first_extract_date, self.meta_key, self.s3_bucket_trg)
        self.meta_update_list = [date for date in self.extract_date_list\
            if date >= self.extract_date]


    def extract(self):
        """Read the source data and concatenates to a pandas data frame
        
        :returns:
        dataframe: Pandas dataframe with the extracted data 
        """
        self._logger.info('Extracting xtera source files started...')
        files = [key for date in self.extract_date_list\
                 for key in self.s3_bucket_src.list_files_in_prefix(date)]
        
        if not files:
            data_frame = pd.DataFrame()
        else:
            data_frame = pd.concat([self.s3_bucket_src.read_csv_to_df(file)\
                                    for file in files],ignore_index=True)
        self._logger.info('Extracting xtera source files started...')
        return data_frame

    def transfor_report1(self):
        pass


    def load(self):
        pass

    def etl_report1(self):
        pass


