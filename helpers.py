#this file contains some functions that I have used to control some particular cases of our data
from pandas import DataFrame
#to remove integers, float, N/A from dataframe
def cleaning_dataframe(df:DataFrame):
        for series in df:
                if series.dtypes == 'int64':
                    df[series].astype("string")
                if series.dtypes == 'float64':
                    df[series].apply(remove_dotzero)
                if df[series].dtypes == 'N/A':
                    df[series].fillna("")
        return df

def remove_dotzero(s):
        return s.replace(".0", "")

#escape quotes if in the label file (for sparql queries (and maybe sql too))
def escape_quotes(string:str):
    if '"' in string:
        return string.replace('"', '\"')
    else:
        return string

#restart the 3 counter.txt files
def clear_counter():
    with open('collection_counter.txt', 'w') as g:
        g.write('0')

    with open('manifest_counter.txt', 'w') as h:
        h.write('0')

    with open('canvas_counter.txt', 'w') as i:
        i.write('0')
