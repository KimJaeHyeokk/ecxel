import pandas as pd
from datetime import datetime, timedelta
import math
import pymysql

# 파일명
file_name = '과수원100.xlsx'

# Daraframe형식으로 엑셀 파일 읽기
saltern_df = pd.read_excel(file_name, sheet_name = 'Sheet2')
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
        data_list.extend(select_row.values.tolist()[0])

    data_list = [value for value in data_list if value is not None and not math.isnan(value)]
    return data_list
        # print(data_list)


start_date = datetime(2022,6,1)
end_date = datetime(2042,5,31)

# date_id리스트 만들기
def data_id_dict_list(start_date,end_date):
    date_list = []
    current_date = start_date

    while current_date <= end_date:
        date_list.append(int(current_date.strftime('%Y%m%d')))
        current_date += timedelta(days=1)

    return date_list
    # print(data_id_dict_list(start_date,end_date))



#db 셋팅
db_ip = '1.223.240.125'   #그랜드썬 외부 IP
# db_ip = '15.15.15.161'      #그랜드썬 내부 IP
db_port = 3306
db_user = 'grandsun'
db_password = 'grandsun!@'
db_db = 'new_rnd'

def test(values,date_id_id,device_id):
    with pymysql.connect(host=db_ip, port=db_port, user=db_user, password=db_password, db=db_db, charset='utf8') as conn:
            with conn.cursor() as cursor:
                # query = f"INSERT INTO tb_prediction_power (value, date_id, device_id_id, ) VALUES {}"
                values_str = ', '.join([f"({value}, {date_id},{device_id})" for value, date_id in zip(values, date_id_id)])

                query = f"INSERT INTO tb_prediction_power (value, date_id, device_id_id) VALUES {values_str}"
                # print(query)
                try:
                    cursor.execute(query)
                    conn.commit()  # Ensure to commit changes
                except Exception as e:
                    print("Error:", e)
                    print("Query execution failed.")


if __name__ == "__main__":
    # print(data_dict_list(start_row,end_row))
    # print(data_id_dict_list(start_date,end_date))
    values = data_dict_list(start_row,end_row)
    date_ids = data_id_dict_list(start_date,end_date)
    device_id = 13
    test(values,date_ids,device_id)
