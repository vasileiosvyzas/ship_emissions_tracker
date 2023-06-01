import os
import pandas as pd

def excel_to_csv_converter(filename):
    df = pd.read_excel(filename, header=2)
    
    new_extension = os.path.splitext(filename)[0] + '.csv'
    print(new_extension)
    df.to_csv(new_extension, index=False)