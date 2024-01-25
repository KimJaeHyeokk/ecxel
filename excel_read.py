import pandas as pd
from datetime import datetime, timedelta
import math
from dateutil.relativedelta import relativedelta

# 파일명
file_name = '염전25_1.xlsx'

# Daraframe형식으로 엑셀 파일 읽기
saltern_df = pd.read_excel(file_name, sheet_name = 'Sheet1',header = 1 ,index_col = 0, )
# saltern_df = saltern_df.transpose()
# saltern_df = saltern_df.dropna()
# print(saltern_df.columns)
shape = saltern_df.shape

start_row = 0
end_row = saltern_df.shape[0]

# list형태로 데이터 가져오기
def data_dict_list(start_row,end_row):
    data_list = []

    for row_index in range(start_row, end_row):
        select_row = saltern_df.iloc[row_index:row_index + 1, 1:]
    # print(select_row)
    # data_list = select_row.values.tolist()[0]
        data_list.extend(select_row.values.tolist()[0])
    data_list = [value for value in data_list if value is not None and not math.isnan(value)]
    return data_list
        # print(data_list)

start_date = datetime(2022,7,1)
end_date = datetime(2023,6,30)

# date_id리스트 만들기
def data_id_dict_list(start_date,end_date):
    date_list = []
    current_date = start_date
    # month_date = relativedelta(months=1)

    while current_date <= end_date:
        date_list.append(int(current_date.strftime('%Y%m%d')))
        current_date += timedelta(days=1)

    # vertical_dates=[]
    # while current_date <= end_date:
    #     vertical_dates.append(int(current_date.strftime('%Y%m%d')))
    #     current_date += timedelta(days=1)
        

    # for date in vertical_dates:
    #     print(date)

    return date_list
    print(data_id_dict_list(start_date,end_date))


if __name__ == "__main__":
    # print(data_dict_list(start_row,end_row))
    print(data_id_dict_list(start_date,end_date))








