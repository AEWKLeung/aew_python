
import pandas as pd

def files_to_df(FileList):
    if FileList:
        allData=pd.DataFrame()
        for file in FileList:
            fileData=pd.read_excel(file)
            frames=[allData,fileData]
            allData=pd.concat(frames,ignore_index=True)
        return allData.reset_index(drop=True)
    else:
        return None
