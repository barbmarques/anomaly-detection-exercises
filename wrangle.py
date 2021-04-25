import pandas as pd

def wrangle_curr_logs(df):

    '''
    This function performs the following operations on the dataframe containing Codeup's
    curriculum logs.  It renames columns to user-friendly names, drops unneccessary 
    columns, creates a datetime column from 'date' and 'time' and deletes
    the original date and time columns, then it converts datetime to datetime format and 
    sets the column as the index for time series analysis.
    '''
    
    #rename columns to more descriptive names
    df = df.rename(columns = {'name': 'cohort', 'user_id':'user', 'program_id':'program'})
    
    #drop unneccessary columns
    df = df.drop(columns = ['deleted_at','updated_at','created_at','start_date','end_date','slack','cohort_id', 'id'])
    
    #replace program id with names
    df = df.replace({'program': 1}, 'web_dev')
    df = df.replace({'program': 2}, 'staff')
    df = df.replace({'program': 3}, 'data_sci')
    df = df.replace({'program': 4}, 'other')
    
    # concat Date Time columns and drops original columns
    df["datetime"] = df["date"] + ' '+ df["time"] 
    df.drop(columns = ['date','time'], inplace = True)
    
    #create DateTime column
    df.datetime = pd.to_datetime(df.datetime, infer_datetime_format=True)
    
    #set DateTime as index
    df = df.set_index('datetime')
    
    #split time columns
    df['year'] = df.index.year
    df['month'] = df.index.month
    df['day'] = df.index.day
    df['hour'] = df.index.hour
 
    return df
    