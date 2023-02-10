import os
import sys
import json
import pandas as pd
import re

#The script was created for a specific dataset

def convert_xlsx_to_json(self, xlsxFiles, dir):
    if xlsxFiles:
        for xlsx_file in xlsxFiles:
            try:
                file_path_dir = os.path.join(dir, self.split_xlsx, xlsx_file)
                df = pd.DataFrame(
                    pd.read_excel(file_path_dir,
                                  index_col=False,
                                  engine='openpyxl'))
                if not df.empty:
                    filename = os.path.basename(xlsx_file)
                    filename = os.path.splitext(filename)[0]
					#for specific dataset
                    try:
                        if df.columns[0]:
                            df.rename(columns={df.columns[0]: "FECHA"},
                                      inplace=True)
                        if df.columns[0]:
                            df.rename(columns={df.columns[1]: "HORA"},
                                      inplace=True)
                        if df.columns[0]:
                            df.rename(columns={df.columns[2]: "3-EXTERI"},
                                      inplace=True)
                        if df.columns[3]:
                            df.rename(columns={df.columns[3]: "UNIDADES"},
                                      inplace=True)
                    except:
                        pass
                    df.columns = df.columns.str.replace(" ", "")
                    df.columns = df.columns.str.upper()
                    #transform received date to specific date format
                    df[df.columns[0]] = pd.to_datetime(
                        df[df.columns[0]]).dt.strftime(
                            '%Y/%d/%m') if pd.to_datetime(
                                df[df.columns[0]],
                                errors='coerce').notnull().all() else ""
                    #Drop unnamed columns
                    df = df.loc[:, ~df.columns.str.
                                contains('^Unnamed', na=False, case=False)]
                    #Split special characters(), \-_!?:) and numbers (\d+) from location string
                    location = re.split("[(), \-_!?:)(\d+)]+", filename)
                    location_split = location[1].capitalize(
                    ) if filename.startswith(
                        "EXTERIOR") else location[0].capitalize()
                    location_split = "location_name1" if (
                        location_split == "location_name2") else location_split
                    df['LOCATION'] = location_split
                    json_str = df.to_json(orient='records', date_format='iso')
                    parsed = json.loads(json_str)
                    with open(
                            dir + self.split_dir + '{}.json'.format(filename),
                            'w') as json_file:
                        json_file.write(json.dumps(parsed, indent=4))
                os.remove(file_path_dir)
            except Exception as e:
                Temp.ERROR_FILES.append({file_path_dir: e})
                pass

def convert_xls_to_json(self, xlsFiles, dir):
    if xlsFiles:
        for xls_file in xlsFiles:
            print(xls_file)
            try:
                df = pd.read_excel(xls_file, index_col=False, engine="xlrd")
                if not df.empty:
                    try:
                        if df.columns[0]:
                            df.rename(columns={df.columns[0]: "FECHA"},
                                      inplace=True)
                        if df.columns[0]:
                            df.rename(columns={df.columns[1]: "HORA"},
                                      inplace=True)
                        if df.columns[0]:
                            df.rename(columns={df.columns[2]: "3-EXTERI"},
                                      inplace=True)
                        if df.columns[3]:
                            df.rename(columns={df.columns[3]: "UNIDADES"},
                                      inplace=True)
                    except:
                        pass
                    df.columns = df.columns.str.upper()
                    filename = os.path.basename(xls_file)
                    filename = os.path.splitext(filename)[0]
                    # transform received date to specific date format
                    df[df.columns[0]] = pd.to_datetime(
                        df[df.columns[0]]).dt.strftime(
                            '%Y/%d/%m') if pd.to_datetime(
                                df[df.columns[0]],
                                errors='coerce').notnull().all() else ""
                    #Drop unnamed columns
                    df = df.loc[:, ~df.columns.str.
                                contains('^Unnamed', na=False, case=False)]
                    #Split special characters(), \-_!?:) and numbers (\d+) from location string
                    location = re.split("[(), \-_!?:)(\d+)]+", filename)
                    location_split = location[0].capitalize()
                    df['LOCATION'] = location_split
                    json_str = df.to_json(orient='records', date_format='iso')
                    parsed = json.loads(json_str)
                    with open(
                            dir + self.split_dir + '{}.json'.format(filename),
                            'w') as json_file:
                        json_file.write(json.dumps(parsed, indent=4))
                os.remove(xls_file)
            except Exception as e:
                Temp.ERROR_FILES.append({xls_file: e})
                pass

def convert_csv_to_json(self, csvFiles, dir):
    if csvFiles:
        for file_path in csvFiles:
            try:
                df = pd.read_csv(file_path, encoding='unicode_escape')
                if not df.empty:
                    try:
                        if df.columns[0]:
                            df.rename(columns={df.columns[0]: "FECHA"},
                                      inplace=True)
                        if df.columns[0]:
                            df.rename(columns={df.columns[1]: "HORA"},
                                      inplace=True)
                        if df.columns[0]:
                            df.rename(columns={df.columns[2]: "3-EXTERI"},
                                      inplace=True)
                        if df.columns[3]:
                            df.rename(columns={df.columns[3]: "UNIDADES"},
                                      inplace=True)
                    except:
                        pass
                    file = file_path.split(os.sep)[-1]
                    filename = file.split('.')[0]
                    df.columns = df.columns.str.replace(" ", "")
                    df.columns = df.columns.str.upper()
                    # transform received date to specific date format
                    df[df.columns[0]] = pd.to_datetime(
                        df[df.columns[0]]).dt.strftime(
                            '%Y/%d/%m') if pd.to_datetime(
                                df[df.columns[0]],
                                errors='coerce').notnull().all() else ""
                    #Split special characters(), \-_!?:) and numbers (\d+) from location string
                    location = re.split("[(), \-_!?:)(\d+)]+", filename)
                    location_split = location[0].capitalize()
                    location_split = "location_name1" if (
                        location_split == "location_name2") else location_split
                    df['LOCATION'] = location_split
                    json_str = df.to_json(orient='records', date_format="iso")
                    parsed = json.loads(json_str)
                    with open(
                            dir + self.split_dir + '{}.json'.format(filename),
                            'w') as json_file:
                        json_file.write(json.dumps(parsed, indent=4))
                os.remove(file_path)
            except Exception as e:
                Temp.ERROR_FILES.append({file_path: e})
                pass

class Temp():
    ERROR_FILES = []

    def __init__(self):
        self.split_dir = "jsons/"
        self.split_xlsx = "split_xlsx/"

    def format_files(self, dir):
        try:
            list = sys.stdin.readline()
            if not os.path.exists(dir + self.split_dir):
                os.makedirs(dir + self.split_dir)
            if not os.path.exists(dir + self.split_xlsx):
                os.makedirs(dir + self.split_xlsx)
            xlsxFiles = []
            xlsFiles = []
            csvFiles = []
            for root, dirs, files in os.walk(dir):
                for filename in files:
                    if filename.lower().endswith(".xls"):
                        xlsFiles.append(os.path.join(root, filename))
                    if filename.lower().endswith(".csv"):
                        csvFiles.append(os.path.join(root, filename))
            xlsxFiles = [
                file for file in os.listdir(dir + self.split_xlsx)
                if file.endswith('.xlsx')
            ]
            convert_xlsx_to_json(self, xlsxFiles, dir)
            convert_xls_to_json(self, xlsFiles, dir)
            convert_csv_to_json(self, csvFiles, dir)
            sys.stdout.write("Script execution finished successfully")

            print('Script finished execution!')
            print(
                'Unprocessed files with errors: {}\n If there are unprocessed files, check the contents of files in the specified paths'
                .format(FaenTemp.ERROR_FILES))
        except Exception as e:
            print("Exception occurred as {}".format(e))
            return False

fa = Temp()
fa.format_files("/path/to/folder")
