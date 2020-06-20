import os
import pyodbc
import time
import datetime

DRIVER_Q = '{SQL Server}'
SERVER_Q = 'localhost\MSSQLSERVER02'
DATABASE_Q = 'myDB'
TRUSTED_Q = 'yes'
CSTR = f'DRIVER={DRIVER_Q};SERVER={SERVER_Q};DATABASE={DATABASE_Q};TRUSTED_CONNECTION={TRUSTED_Q}'
CONN = pyodbc.connect(CSTR)

FOLDER_NAME = 'Folder'
TB_NAME = 'Table'

FOLDER_PATH = 'D:/Data/'

