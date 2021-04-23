import pandas as pd

def wrangle_curr_logs():

    '''
    This function write the gzip curriculum logs to a dataframe, renames columns to 
    user-friendly names, creates a datetime column from 'date' and 'time' and deletes
    the original date and time columns, then it converts datetime to datetime format and 
    sets the column as the index for time series analysis.
    '''
    
    # write gzip, csv file to dataframe
    df = pd.read_csv('anonymized-curriculum-access.txt.gz', compression='gzip', header=None, sep=' ', quotechar='"', error_bad_lines=False)
    
    #rename columns to more descriptive names
    df = df.rename(columns = {0: "date", 1:"time", 2:"page", 3:"user", 4:"cohort", 5:"ip"})
    
    # concat Date Time columns and drop original columns
    df["datetime"] = df["date"] + ' ' + df["time"] 
    df.drop(columns = ['date','time'], inplace = True)
    
    #create DateTime column
    df.datetime = pd.to_datetime(df.datetime, infer_datetime_format=True)
    
    #set DateTime as index
    df = df.set_index('datetime')
    
    return df
    