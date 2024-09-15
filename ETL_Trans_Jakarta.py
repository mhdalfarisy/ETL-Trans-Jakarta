import pandas as pd
import os
import logging
import traceback
from datetime import datetime
from pandas_gbq import read_gbq

folder_log = 'Path Data Source'
file_log_name = 'Log_Testing_Script_3.log'
file_log_csv_name = 'Log_Testing_Script_3.csv'
file_log = os.path.join(folder_log,'file_name.log')
file_log_csv = os.path.join(folder_log,'file_name.csv')
print(f'Folder Log : {folder_log} \n File log in {file_log_name} and Log csv : {file_log_csv_name}')

logging.basicConfig(filename=file_log,
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logging.info(f'Configure Log Finish. . .')
print('Configure Log Done. . .')

try:
    logging.info('Start Read File CSV')
    start_read_file_csv = datetime.now()
    print(f'Start Read File CSV {datetime.now()}')
    folder_csv = 'Path Data Source'
    file_csv_name = 'Transjakarta.csv'
    file_csv = pd.read_csv(os.path.join(folder_csv,file_csv_name))
    print(file_csv.head())
    finish_read_file_csv = datetime.now()
    print(f'finish_read_file_csv {finish_read_file_csv}')
    
    project_id_bigquery = 'ID Project'
    datawarehouse_name = 'TransJakarta'
    datawarehouse_table_name = 'TableMaster'
    datawarehouse_data = f'{project_id_bigquery}.{datawarehouse_name}.{datawarehouse_table_name}' 
    file_csv.to_gbq(datawarehouse_data, project_id=project_id_bigquery, if_exists='replace')
    
    logging.info('Start Send to Datawarehouse')
    start_send_to_bigquery = datetime.now()
    print(f'Finish Send to Datawarehouse... {datetime.now()}')
    finish_send_to_bigquery = datetime.now()
    logging.info('Finish Send to Datawarehouse')

    upload_file_time = finish_send_to_bigquery - start_send_to_bigquery
    minute,second = divmod(upload_file_time.total_seconds(),60)
    duration_str = f'{int(minute)} minute {int(second)} second'
    
    file_log_csv_information = {
        'File': [file_log_csv],
        'Rows' : [file_csv.shape[0]],
        'Columns' : [file_csv.shape[1]],
        'Start Read File CSV' : [start_read_file_csv.strftime('%Y-%m-%d %H:%M:%S')],
        'Finish Read File CSV' : [finish_read_file_csv.strftime('%Y-%m-%d %H:%M:%S')],
        'Start Send to Bigquery' : [start_send_to_bigquery.strftime('%Y-%m-%d %H:%M:%S')],
        'Finish Send to Bigquery' : [finish_send_to_bigquery.strftime('%Y-%m-%d %H:%M:%S')],
        'Total Duration Upload to Bigquery' : [duration_str],
        'Status' : ['Complete']}
    
    df_file_log_csv_information = pd.DataFrame(file_log_csv_information)
    df_file_log_csv_information.to_csv(file_log_csv, mode='a', index=False, header=not os.path.join(file_log_csv))
    print('Finish send to File Log CSV Information...')
     
except Exception as e :
    error_message = traceback.format_exc()
    logging.error(f'Error occured : {error_message}')
    print(f'Error occured : {error_message}')
    
    logging.error('Error')
    print('Error send to datawarehouse')
    
    file_log_csv_information = {
        'File' : [file_log_csv],
        'Rows' : [file_csv.shape[0]],
        'Columns' : [file_csv.shape[1]],
        'Start Read File CSV' : [start_read_file_csv.strftime('%Y-%m-%d %H:%M:%S')],
        'Finish Read File CSV' : [finish_read_file_csv.strftime('%Y-%m-%d %H:%M:%S')],
        'Start Send to Bigquery' : [start_send_to_bigquery.strftime('%Y-%M-%d %H:%M:%S')],
        'Finish Send to Bigquery' : [finish_send_to_bigquery.strftime('%Y-%M-%d %H:%M:%S')],
        'Total Duration Upload to Bigquery' : [duration_str],
        'Status' : [f'Error : {error_message}']}

    df_file_log_csv_information = pd.DataFrame(file_log_csv_information)
    df_file_log_csv_information.to_csv(file_log_csv, mode='a', index=False, header=not os.path.join(file_log_csv))
    print('Have Error')