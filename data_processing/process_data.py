from importlib.util import set_loader
import os
import pandas as pd
import datetime
import itertools


# df.drop(df.columns[[0]], axis=1, inplace=True)
# df.drop(["Total Hour", "Note"], axis=1, inplace=True)
# print(df.head())


# for i, row in df.iterrows():
#     if i == 50:
#         break
#     if row["In Time"] == "-":
#         continue
#     temp_data_dictionary["User ID"] = row["User ID"]
#     temp_data_dictionary["User Name"] = row["User Name"]
#     temp_data_dictionary["Date"] = row["Date"]
#     temp_data_dictionary["Time"] = row["In Time"]
#     initally_processed_df = pd.concat([initally_processed_df, pd.DataFrame.from_records([temp_data_dictionary])])
    
#     if row["Out Time"] == "-":
#         temp_data_dictionary["Time"] = "11:59 PM"
#         initally_processed_df = pd.concat([initally_processed_df, pd.DataFrame.from_records([temp_data_dictionary])])
#         temp_data_dictionary["Date"] = datetime.datetime.strftime(datetime.datetime.strptime(temp_data_dictionary["Date"], "%Y-%m-%d") \
#                                                         + datetime.timedelta(days = 1), "%Y-%m-%d")
#         temp_data_dictionary["Time"] = "12:00 AM"
#         initally_processed_df = pd.concat([initally_processed_df, pd.DataFrame.from_records([temp_data_dictionary])])
#     else:
#         temp_data_dictionary["Time"] = row["Out Time"]
#         initally_processed_df = pd.concat([initally_processed_df, pd.DataFrame.from_records([temp_data_dictionary])])

# for i, g in df.groupby(["User ID"]):
#     for j, row in g.iterrows():
#         if row["In Time"] == "-":
#             continue
#         temp_data_dictionary["User ID"] = row["User ID"]
#         temp_data_dictionary["User Name"] = row["User Name"]
#         temp_data_dictionary["Date"] = row["Date"]
#         temp_data_dictionary["Time"] = row["In Time"]
#         initally_processed_df = pd.concat([initally_processed_df, pd.DataFrame.from_records([temp_data_dictionary])])
#         if row["Out Time"] == "-":
#             continue
#         temp_data_dictionary["Time"] = row["Out Time"]
#         initally_processed_df = pd.concat([initally_processed_df, pd.DataFrame.from_records([temp_data_dictionary])])        
        
# print(initally_processed_df.head())
# initally_processed_df.to_csv(current_working_directory+data_folder+"processed.csv")

# finally_processed_df = pd.DataFrame(columns= initally_processed_df_columns)


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
                
                
if __name__=="__main__":
    current_working_directory = os.getcwd()
    data_folder = "/data_processing/data/"
    file_name = "Nextzen Attendance Management Portal.xlsx"

    df = pd.read_excel(current_working_directory + data_folder + file_name)
    print(df.head())
    data_processor = ProcessInputData(df=df, drop_columns=["Unnamed: 0", "Total Hour", "Note"])
    data_processor.convertToRawData()
    # print(data_processor.processed_df.head())
    # print("size before: ",data_processor.processed_df.shape[0])
    # data_processor.eliminateProblemRows(2)
    # print("size after: ",data_processor.processed_df.shape[0])
    print(data_processor.problem_df.head())