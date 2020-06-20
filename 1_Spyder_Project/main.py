from data_config import *
from create_db import Create_DB
from time import time
import logging

logging.basicConfig(level= ogging.DEBUG, filename='myLog.log', filemode='a'
                    , format='%(asctime)s %(levelname)s: %(message)s')

def main_create_db():
    try:
        Create_DB().Auto_Create()
        logging.info('<< Table Created')
    except Exception as e:
        print(e)
        Create_DB().Delete_DB()
        logging.info('<< Database Deleted')
    finally:
        res = Create_DB().Insert_DB()
        logging.info('<< Data Inserted')
    return res
    
if __name__ == '__main__':
    result = main_create_db()
