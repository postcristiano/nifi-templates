import sys
import pandas as pd
from io import StringIO
import numpy as np
pd.options.mode.chained_assignment = None

class AsmCurrentHarmonizator():
    
    def __init__(self):
        self.id = sys.argv[1]
        # NOTE default sampling period, might want to change
        self.sampling_period = 5
        # self.raw_data = StringIO(sys.stdin.read())
        self.raw_data = sys.stdin.read()


    def __load_meter_data__(self, phase):

        # suffixes = [".Wh", ".VArh"]
        if(phase=="l1"): 
            tagname = "Current_l1_rms_31_7_0_"
            # suffix = ".kWh"
        if(phase=="l2"):
            tagname = "Current_l2_rms_51_7_0_"
            # suffix = ".kVArh"
        if(phase=="l3"):
            tagname = "Current_l3_rms_71_7_0_"

        meter_data = pd.DataFrame()
        df = pd.read_csv(StringIO(self.raw_data), header=None, names=["timestamp", "tagname", "current", "quality", "qualityDetail", "OPCquality"])
        # NOTE: Metertag does not work in NiFi for some reason!
        #selecting tagnames for active or reactive:
        #metertag = []
       
        df = df.loc[ (df["tagname"].str.startswith(tagname)) ]
        # df = df[df["tagname"].isin(metertag)]

        #reformating timestamps to python datetimes
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        #setting a datetime index:
        df.set_index("timestamp", inplace=True)

    
        #resample data in 5 minute intervals
        df = df.resample(str(self.sampling_period)+"min").agg({"current":np.sum, "quality":np.max, "qualityDetail":np.sum, "OPCquality":np.min})
        #create new column to indicate if there was any data during this 1 hour interval
        df["isnull"] = df["quality"].isnull()  
        #creating column with the meter id:  
        df["id"] = self.id

        #resetting the index (not datetime index, so that in the dataframe with all the smart meters every index is unique)
        df = df.reset_index()

        meter_data = df
        meter_data["timestamp"] = pd.to_datetime(meter_data["timestamp"])
        # meter_data = meter_data[meter_data.timestamp >= "2021-09-01 00:00:00"]
        # meter_data.reset_index()
        meter_data.set_index("timestamp", inplace=True)
        return meter_data


    def __data_preprocessing__(self, meter_data, phase):
        to_export = pd.DataFrame()

        one_meter = meter_data.copy()
        one_meter.loc[one_meter["isnull"]==True, "current"] = np.nan

        #now select all days that have no null values after imputation
        one_meter = self.__reject_empty_days__(one_meter)
        
        one_meter = one_meter[["current"]]
        one_meter["date"] = one_meter.index.date
        one_meter["time"] = one_meter.index.time
        one_meter = one_meter.pivot(index="date", columns="time", values="current")
        one_meter["id"] = self.id

        to_export = pd.concat([to_export,one_meter])
        to_export["phase"] = phase
        to_export = to_export.reset_index()
        return(to_export)

    def __reject_empty_days__(self, meter_data):
        meter_data["date"] = meter_data.index.date
        meter_data["time"] = meter_data.index.time
        not_empty_days = meter_data.pivot(index="date", columns ="time", values="isnull")
        not_empty_days = not_empty_days.loc[~not_empty_days[not_empty_days.columns].all(True)]
        meter_data = meter_data[meter_data["date"].isin(not_empty_days.index)]
        return(meter_data)


    def execute(self):
        meter_data = self.__load_meter_data__("l1")
        l1_csv = self.__data_preprocessing__(meter_data, "l1")
        meter_data = self.__load_meter_data__("l2")
        l2_csv = self.__data_preprocessing__(meter_data, "l2")
        meter_data = self.__load_meter_data__("l3")
        l3_csv = self.__data_preprocessing__(meter_data, "l3")

        total_csv = pd.concat([l1_csv, l2_csv, l3_csv])
        total_csv = total_csv.reset_index()
        total_csv.drop(columns=["index"], inplace=True)
        
        ret_string = StringIO()
        total_csv.to_csv(ret_string)
        sys.stdout.write(ret_string.getvalue())



def main():
    harmonizator = AsmCurrentHarmonizator()
    harmonizator.execute()

if __name__ == "__main__":
    main()