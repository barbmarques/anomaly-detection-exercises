import pandas as pd

def acquire(file_name, column_names):
    '''
    This function takes in a .csv file and column_names and returns the .csv written 
    to a dataframe. The function uses only 5 columns (0, 2, 3, 4, 5) of the original 
    .csv file and removes the header row of the .csv file and replaces it with given 
    column_names
    '''
    return pd.read_csv(file_name, sep = '\s', header=None, names = column_names, usecols=[0, 2, 3, 4, 5])

def prep(df, user): 
    '''
    This functions takes in a dataframe and user_id number, preps the 
    data by creating a dataframe for an individual user, setting the date
    as a datetime index. It returns a count of the user's endpoints by day"
    '''
    # create a dataframe containing only the given user_id
    df = df[df.user_id == user]
    
    # convert date to datetime datatype
    df.date = pd.to_datetime(df.date)
    
    # reset dataframe index to date
    df = df.set_index(df.date)
    
    # create a series of the count of user endpoints by day
    pages = df['endpoint'].resample('d').count()
    
    return pages


def compute_pct_b(pages, span, weight, user):
    '''
    This function takes in a series, date span, bollinger weight and user_id
    and returns a dataframe containing the user_id, pages, the upper, lower and 
    mid Bollinger Bands and %b (percent bandwidth)
    '''
    
    # calculate the midband, or exponential moving average
    midband = pages.ewm(span=span).mean()
    
    # calculate standard deviation of pages data
    stdev = pages.ewm(span=span).std()
    
    # calculate the upper and lower bands based on std and given weight
    ub = midband + (stdev * weight)
    lb = midband - (stdev * weight)
    
    # create a dataframe of pages and bollinger bands
    bb = pd.concat([ub, lb], axis=1)
    my_df = pd.concat([pages, midband, bb], axis=1)
    my_df.columns = ['pages','midband','ub','lb']
    
    # calculate percent bandwidth (%b) and add to dataframe
    my_df['pct_b'] = (my_df['pages'] - my_df['lb']) / (my_df['ub'] - my_df['lb'])
    
    # create column for user id
    my_df['user_id'] = user
    
    return my_df


def plt_bands(my_df, user):
    '''
    This function takes in a dataframe and user id and returns a plot of number of
    pages accessed by the user over time, including Bollinger bands
    '''
    
    # set figure size
    fig, ax = plt.subplots(figsize=(12,8))
    
    # plot columns and labels
    ax.plot(my_df.index, my_df.pages, label='Number of Pages, User: '+str(user))
    ax.plot(my_df.index, my_df.midband, label='EMA/midband')
    ax.plot(my_df.index, my_df.ub, label='Upper Band')
    ax.plot(my_df.index, my_df.lb, label='Lower Band')
    
    # set legend and ylabel
    ax.legend(loc='best')
    ax.set_ylabel('Number of Pages')
    
    plt.show()
    
    
    
def find_anomalies(df,user, span, weight):
    '''
    This function takes in a dataframe, user id, date span and Bollinger weight and 
    returns a dataframe of anomalies (with values where pct_b>1).  This function 
    relies on helper functions: prep(), compute_pct_b().
    '''
    
    # return a series with a count of the user's endpoints by day
    pages = prep(df, user)
    
    # create a dataframe  of user_id, pages, the upper, lower and mid Bollinger Bands and %b (percent bandwidth)
    my_df = compute_pct_b(pages, span, weight, user)
    
    # return dataframe of anomalies for user
    return my_df[my_df.pct_b>1]