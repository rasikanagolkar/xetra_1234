"""Xetra ETL Component"""

import logging
from typing import NamedTuple
from xetra.common.S3 import S3BucketConnector


class XetraSourceConfig(NamedTuple):
    """
    Class for source configuration data
    
    """

    src_first_extract_date: str
    sc_columns: str
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_price: str


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
        self.extract_date = 
        self.extract_date_list = 
        self.meta_update_list = 


    def extract(self):
        pass


    def transfor_report1(self):
        pass


    def load(self):
        pass

    def etl_report1(self):
        pass


