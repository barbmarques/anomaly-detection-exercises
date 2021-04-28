#basic imports
import pandas as pd
import numpy as np
import env

import env

# connection function for accessing mysql 
def get_connection(db, user=env.user, host=env.host, password=env.password):
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def acquire_curr_logs():
    '''
    This function connects to Codeup's SQL Server using given parameters in the user's
    env file.  It then uses a SQL query to acquire all data (two tables) from Codeup's curriculum 
    access logs database on curriculum. It joins the tables and returns all 
    data in a single dataframe called df. 
    '''
    
    def get_connection(db, user=env.user, host=env.host, password=env.password):
         return f'mysql+pymysql://{user}:{password}@{host}/{db}'
    query = '''
           SELECT * 
           FROM logs 
           LEFT JOIN cohorts on logs.cohort_id = cohorts.id;
            '''

    df = pd.read_sql(query, get_connection('curriculum_logs'))
    
    return df


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


def web_dev_pages():
    '''
    This function connects to Codeup's SQL Server using given parameters in the user's
    env file.  It then uses a SQL query to acquire pages/paths from Codeup's curriculum 
    that are being accessed by web dev students. 
    '''
    
    def get_connection(db, user=env.user, host=env.host, password=env.password):
         return f'mysql+pymysql://{user}:{password}@{host}/{db}'
    query = '''
            SELECT path, Count(*) 
            FROM logs
            JOIN cohorts ON logs.cohort_id = cohorts.id
            WHERE (program_id = 1)
            GROUP BY path
            ORDER BY count(*) DESC
            '''

    df = pd.read_sql(query, get_connection('curriculum_logs'))
    
    return df

