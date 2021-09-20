import pandas as pd
import os
import sys
import shutil

pd.set_option('mode.chained_assignment', None)

# set the csv file contains the file names
CSV_PATH = '/Users/xxxxx/Downloads'
RENAME_CSV_FILE_NAME = 'data.csv'
SOURCE_FOLDER = '/Users/xx/Downloads/101_ObjectCategories/butterfly'
TARGET_FOLDER = '/Users/xx/Downloads/101_ObjectCategories/butterfly_renamed'
# Source file analysis
'''
1. how many files?
2. Is file data same even though name is different ? # only based on os.stats, advanced version is hash Codes!!
'''
file_with_abs_path = CSV_PATH + os.sep + RENAME_CSV_FILE_NAME


class SourceAnalysis:

    def __init__(self, source_dir):
    
        self.source_folder = source_dir

    # @classmethod
    def source_analysis(self):
        files_in_dir = os.listdir(self.source_folder)
        # files_in_dir = [file for file in os.listdir(self.source_folder) if os.path.isfile(file)]
        col_names = ['file_name', 'file_stats', 'file_abs_path']
        df_files = pd.DataFrame(columns=col_names)
        # dictionary to save file and its states
        file_stat = {}
        for each_file in files_in_dir:
            file_stat['file_name'] = each_file
            os_stats = os.stat(self.source_folder + os.sep + each_file)
            file_stat['file_abs_path'] = self.source_folder + os.sep + each_file
            file_stat['file_stats'] = str(os_stats.st_size + os_stats.st_mtime)
            df_files = df_files.append(file_stat, ignore_index=True)
            file_stat.clear()
        df_duplicate = df_files.groupby('file_stats').filter(lambda x: len(x) > 1)
        return df_files, df_duplicate


if __name__ == "__main__":
    source = SourceAnalysis(SOURCE_FOLDER)
    df, df_duplicates = source.source_analysis()
    df.to_csv("files.csv")
    print("+"*50 + "Source folder analysis"+"+" * (50 - len("Source folder analysis")))
    print("Total files in source folder:  " + str(df.shape[0]))
    print("---->" + "For Prob duplicates files refer: file_which_may_be_having_duplicate_content.csv " + "<------")
    df_duplicates.to_csv('files_which_may_be_having_duplicate_content.csv')
    print("# of files in duplicates csv: " + str(df_duplicates.shape[0]))
    print("+" * 100)
    print("\n")
    print('Checking rename target folder....')
    if os.path.isdir(TARGET_FOLDER):
        print("Target folder: ", TARGET_FOLDER, "exits!!!!")
        files = os.listdir(TARGET_FOLDER)
        print("As folder exist, do you want to delete files ? type Y for yes!")
        approved = input()
        if approved != 'Y':
            print("System is terminating the process...")
            sys.exit(1)

        for file in files:
            os.remove(TARGET_FOLDER + os.sep + file)

    else:
        print("Creating the folder...")
        os.mkdir(TARGET_FOLDER)
        print("Folder:", TARGET_FOLDER, "created!")

    # read the rename file

    df_rename = pd.read_csv(file_with_abs_path)
    #print(df_rename.head(1))
    for index, row in df_rename.iterrows():
        print("Processing file:", row['OldImageName'], end="")

        try:
            df_temp = df.loc[df['file_name'] == row['OldImageName']]
        except Exception as e:
            print(":Exception found, file skipped")
            continue

        df_temp.file_abs_path = df_temp.file_abs_path.astype('str')
        df_temp.file_abs_path = df_temp.file_abs_path.apply(lambda x: str(x))
        # print(df_temp.dtypes)

        if df_temp.shape[0] > 1:
            print(":For this file multiple entries found in source so skipped from copy and rename")
            continue
        elif df_temp.shape[0] == 1:
            # print("Path is ---> ", TARGET_FOLDER+os.sep + row['NewImageName'])
            path_with_spaces =  TARGET_FOLDER+os.sep + row['NewImageName']
            value = df_temp['file_abs_path']
            source_path = ""
            for each in value:
                source_path = each

            # print("File path:"+df_temp['file_abs_path'].to_string())
            try:
                shutil.copy2(source_path, path_with_spaces)
                print (": Success!!")
            except Exception as e:
                print(": Exception found")
        else:
            print(":", "File Not Found for renaming")

        df_temp = df_temp[0:0]
