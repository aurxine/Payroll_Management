from importlib.util import set_loader
import os
import pandas as pd
import datetime
import itertools


def roundTime(dt=None, roundTo=60):
   """Round a datetime object to any time lapse in seconds
   dt : datetime.datetime object, default now.
   roundTo : Closest number of seconds to round to, default 1 minute.
   Author: Thierry Husson 2012 - Use it as you want but don't blame me.
   """
   if dt == None : dt = datetime.datetime.now()
   seconds = (dt.replace(tzinfo=None) - dt.min).seconds
   rounding = (seconds+roundTo/2) // roundTo * roundTo
   return dt + datetime.timedelta(0,rounding-seconds,-dt.microsecond)

def round_time(dt=None, date_delta=datetime.timedelta(minutes=1), to='average'):
    """
    Round a datetime object to a multiple of a timedelta
    dt : datetime.datetime object, default now.
    dateDelta : timedelta object, we round to a multiple of this, default 1 minute.
    from:  http://stackoverflow.com/questions/3463930/how-to-round-the-minute-of-a-datetime-object-python
    """
    round_to = date_delta.total_seconds()
    if dt is None:
        dt = datetime.now()
    seconds = (dt - dt.min).seconds

    if seconds % round_to == 0 and dt.microsecond == 0:
        rounding = (seconds + round_to / 2) // round_to * round_to
    else:
        if to == 'up':
            # // is a floor division, not a comment on following line (like in javascript):
            rounding = (seconds + dt.microsecond/1000000 + round_to) // round_to * round_to
        elif to == 'down':
            rounding = seconds // round_to * round_to
        else:
            rounding = (seconds + round_to / 2) // round_to * round_to

    return dt + datetime.timedelta(0, rounding - seconds, - dt.microsecond)

class ProcessInputData():
    def __init__(self, df:pd.DataFrame=None, drop_columns:list=None) -> None:
        self.input_df = df
        self.df = self.input_df.copy()
        self.processed_df = pd.DataFrame()
        self.problem_df = pd.DataFrame()
        self.problem_rows = []
        if df is not None and drop_columns is not None:
            try:
                self.df.drop(drop_columns, axis = 1, inplace=True)
            except:
                print("error while dropping columns")
        
    
    def convertToRawData(self, group:list=["User ID"], columns_to_check:list=["In Time", "Out Time"],\
                        ignore_mark:list=["-"], new_columns:list=["User ID", "User Name", "Date", "Time"],\
                            mapping:dict={"In Time":"Time", "Out Time":"Time"}, time_threshold:int=300):
        
        temp_data_dictionary = dict.fromkeys(new_columns)
        index_count = 0 # There must be a better way than this
        
        for i, g in self.df.groupby(group):
            
            # define datetime variables for checking time issues
            last_datetime = None
            current_datetime = None
            
            for j, row in g.iterrows(): # filter df by user id
                for key in row.keys(): # iterate through rows
                    if key in temp_data_dictionary.keys(): # Add all the other column data
                        temp_data_dictionary[key] = row[key]
                        
                for key in row.keys():
                    # In this loop, the columns which are being checked are inserted into the dictionary.
                    # The mapping dictionary maps the column to it's intended new column
                    if key in columns_to_check and row[key] not in ignore_mark:
                        temp_data_dictionary[mapping[key]] = row[key]
                        
                        # Now checking whether subsequent timestamps are apart by more than the time_threshold (default 5 mins)
                        # This is to eliminate multiple checkin by the user
                        if last_datetime is None:
                            last_datetime = datetime.datetime.strptime(temp_data_dictionary["Date"] + " " + temp_data_dictionary["Time"], \
                                                                        "%Y-%m-%d %I:%M:%S %p")
                        else:
                            current_datetime = datetime.datetime.strptime(temp_data_dictionary["Date"] + " " + temp_data_dictionary["Time"], \
                                                                        "%Y-%m-%d %I:%M:%S %p")
                            time_delta = current_datetime - last_datetime
                            if time_delta.total_seconds() <= time_threshold:
                                self.problem_rows.append((index_count - 1, index_count))
                            last_datetime = current_datetime
                            
                        index_count += 1
                        self.processed_df = pd.concat([self.processed_df, pd.DataFrame.from_records([temp_data_dictionary])], ignore_index=True)
        
        # Copy the problematic rows into a new df 1         
        self.problem_df = self.processed_df.iloc[list(itertools.chain.from_iterable(self.problem_rows))]
        
    def eliminateProblemRows(self, tuple_index:int=0):
        '''
        Eliminates the rows that are problematic
        
        tuple_index = 0 : Eliminates the first index from the problem_rows tuple
        tuple_index = 1 : Eliminates the second index from the problem_rows tuple
        tuple_index = 2 : Eliminates both index from the problem_rows tuple
        '''
        
        if tuple_index == 2:
            # to do: using keep_indexes
            self.processed_df.drop(self.processed_df.index[list(itertools.chain.from_iterable(self.problem_rows))], axis=0, inplace=True)
            
        elif tuple_index in [0, 1]:
            self.processed_df.drop(self.processed_df.index[[x[tuple_index] for x in self.problem_rows]], axis=0, inplace=True)
        else:
            print("Index out of range")
            
            
    def roundDateTime(self, nearest_minutes = 20):
        # self.processed_df[column_name] = roundTime(self.processed_df[column_name], nearest_minutes)
        # self.processed_df[column_name].mask(self.processed_df[column_name], roundTime(self.processed_df[column_name], nearest_minutes), inplace=True)
        for i, row in self.processed_df.iterrows():
            unrounded_date = row["Date"]
            unrounded_time = row["Time"]
            unrounded_date_time = datetime.datetime.strptime(unrounded_date + " " + unrounded_time, "%Y-%m-%d %I:%M:%S %p")
            rounded_date_time = round_time(unrounded_date_time, datetime.timedelta(minutes=nearest_minutes), to="average")
            rounded_date, rounded_time, p = rounded_date_time.strftime("%Y-%m-%d %I:%M:%S %p").split()
            rounded_time += " " + p
            
            self.processed_df.at[i, "Date"] = rounded_date
            self.processed_df.at[i, "Time"] = rounded_time
                
if __name__=="__main__":
    current_working_directory = os.getcwd()
    data_folder = "/data/"
    file_name = "Nextzen Attendance Management Portal-July.xlsx"

    df = pd.read_excel(current_working_directory + data_folder + file_name)
    print(df.head())
    data_processor = ProcessInputData(df=df, drop_columns=["Unnamed: 0", "Total Hour", "Note"])
    data_processor.convertToRawData()
    # print("size before: ",data_processor.processed_df.shape[0])
    print(data_processor.problem_df.head())
    data_processor.eliminateProblemRows(1)
    # print("size after: ",data_processor.processed_df.shape[0])
    print(data_processor.processed_df.head())
    # data_processor.processed_df.to_csv(current_working_directory + data_folder + "july_data.csv")
    data_processor.roundDateTime()
    print(data_processor.processed_df.head())
    # data_processor.processed_df.to_csv(current_working_directory + data_folder + "july_data_rounded.csv")