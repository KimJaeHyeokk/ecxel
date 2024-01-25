from datetime import datetime
import time
import json
import socket
import json
import pymysql
#db 셋팅
db_ip = '1.223.240.125'
db_port = 3306
db_user = 'grandsun'
db_password = 'grandsun!@'
db_db = 'new_rnd'
# 정의:
# n_gunwi_field_sensor = 실증지 3번 
# n_gunwi_paddy_sensor = 실증지 2번     #module_temp_1, # soil_temp_2 실증지 2,3,4
# n_gunwi_orchard_sensor = 실증지 4번
# n_gch_sensor = 실증지 5번             #module_temp_3 # soil_temp_4 실증지 5
# n_muan_sensor = 실증지 1번            #module_temp_3 # soil_temp_5 실증지 1
TB_demon_area_id_1 = 'n_muan_sensor'
TB_demon_area_id_2 = 'n_gunwi_paddy_sensor'
TB_demon_area_id_3 = 'n_gunwi_field_sensor'
TB_demon_area_id_4 = 'n_gunwi_orchard_sensor'
TB_demon_area_id_5 = 'n_gch_sensor'


class DB():
    def selectQuery_rnd(query, distinct=None):
        connection = pymysql.connect(
            host = '1.223.240.125',
            port = 3306,
            db = 'rnd',
            user = 'grandsun',
            password = 'grandsun!@',
            charset = 'utf8'
        )
        with connection.cursor() as cursor:
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]

            queryset = []
            temp = None
            for row in cursor.fetchall():
                if distinct != None:    #중복제거
                    if temp != row[distinct]:
                        queryset.append(dict(zip(columns, row)))
                        temp = row[distinct]
                else:
                    queryset.append(dict(zip(columns, row)))
            return queryset
    def selectQuery_new_rnd(query, distinct=None):
        connection = pymysql.connect(
            host = '1.223.240.125',
            port = 3306,
            db = 'new_rnd',
            user = 'grandsun',
            password = 'grandsun!@',
            charset = 'utf8'
        )
        with connection.cursor() as cursor:
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]

            queryset = []
            temp = None
            for row in cursor.fetchall():
                if distinct != None:    #중복제거
                    if temp != row[distinct]:
                        queryset.append(dict(zip(columns, row)))
                        temp = row[distinct]
                else:
                    queryset.append(dict(zip(columns, row)))
            return queryset
    def data_db_save(table_name, data_set):
        with pymysql.connect(host=db_ip, port=db_port, user=db_user, password=db_password, db=db_db, charset='utf8') as conn:
            with conn.cursor() as cursor:
                try:
                    for data in data_set:
                        data['create_datetime'] = datetime.strptime(data['create_datetime'], '%Y-%m-%d %H:%M:%S')

                    # Use parameterized query to prevent SQL injection
                    query = f"INSERT INTO {table_name} " \
                            "(sun_1, sun_2, sun_3, sun_4, sun_5, module_temp_1, module_temp_3, soil_temp_2, soil_temp_4, " \
                            "soil_temp_5, wd_n, wd_t, ws, ws_1minute_max, date_id, create_datetime, demon_area_id_id) " \
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    # VALUES(~~),(~~),(~~),(~~)
                    values = [
                        (
                            d['sun_1'], d['sun_2'], d['sun_3'], d['sun_4'], d['sun_5'], d['module_temp_1'],
                            d['module_temp_3'], d['soil_temp_2'], d['soil_temp_4'], d['soil_temp_5'],
                            d['wd_n'], d['wd_t'], d['ws'], d['ws_1minute_max'], d['date_id'],
                            data['create_datetime'], d['demon_area_id_id']
                        ) for d in data_set
                    ]

                    cursor.executemany(query, values)
                    conn.commit()  # Ensure to commit changes

                except Exception as e:
                    print("Error:", e)
                    print("Query execution failed.")
    # def time_process(date,time)
    def key_setting(data_set):
        demon_area_id_id = data_set[0]['demon_area_id_id']
        table_name = DB.Demonstration_Area_table_Check(demon_area_id_id)
        final_data_set = []
        print('총 갯수 : ',len(data_set))
        count = 0
        for data_set_dic in data_set:
            count = count + 1
            print(f'몇번째야? : {count}')
            device_id = data_set_dic['device_id']
            # 인버터에 센싱할 데이터가 아닌 대상들 딕셔너리에서 제외
            create_datetime = data_set_dic['create_datetime']
            date_id = data_set_dic['date_id']
            delete_keys = ['demon_area_id_id','device_id','create_datetime','date_id']   
            for delete_key in delete_keys:
                if delete_key in data_set_dic:
                    del data_set_dic[delete_key]
            result = []
            for key, value in data_set_dic.items():
                parts = key.rsplit('_', 1)
                if len(parts) == 2 and parts[1].isdigit():
                    name = parts[0]
                    label = int(parts[1])
                    result.append({"name": name, "label": label, "value": value})
            query = 'SELECT id, name FROM tb_info_inv_data'
            query_set = DB.selectQuery_new_rnd(query)   # id, 이름
            for data in result:
                for info in query_set:
                    if data['name'] == info['name']:
                        data_info_id_id = info['id']
                        composition_query = f'SELECT id FROM tb_info_data_composition WHERE data_label = {data["label"]} AND data_info_id_id = {data_info_id_id} AND demon_area_id_id = {demon_area_id_id} AND device_id_id = {device_id}'
                        composition_query_set = DB.selectQuery_new_rnd(composition_query)
                        final_data_set.append({'value':data['value'], 'date_id':date_id,'create_datetime':create_datetime,'data_id_id':composition_query_set[0]['id']})
                        break
        DB.Inv_data_db_save(table_name,final_data_set)  
    def Inv_data_db_save(table_name, final_data_set):
        print('INV DATA SAVE START')
        with pymysql.connect(host=db_ip, port=db_port, user=db_user, password=db_password, db=db_db, charset='utf8') as conn:
            with conn.cursor() as cursor:
                try:
                    for data in final_data_set:
                        data['create_datetime'] = datetime.strptime(data['create_datetime'], '%Y-%m-%d %H:%M:%S')

                    # Use parameterized query to prevent SQL injection
                    query = f"INSERT INTO {table_name} " \
                            "(value,date_id,create_datetime,data_id_id) " \
                            "VALUES (%s, %s, %s, %s)"
                    values = [
                        (
                            d['value'], d['date_id'], d['create_datetime'], d['data_id_id']
                        ) for d in final_data_set
                    ]

                    cursor.executemany(query, values)
                    conn.commit()  # Ensure to commit changes

                except Exception as e:
                    print("Error:", e)
                    print("Query execution failed.")
    def select_tb_demonstration(table_name):
        query = f'SELECT * FROM {table_name}'
        query_set = DB.selectQuery_rnd(query)
        return query_set
    def change_name(data_dict,new_name,existing_name):
        if existing_name in data_dict: # 이 existing_name이라는 키가 data_dict에 키가 존재할 경우
            data_dict[new_name] = data_dict.pop(existing_name)
        else:   # data_dict 이 키가 존재하지 않는경우는 
            data_dict[new_name] = None
    
    def data_dict_organize(demon_area_id_id,data_dict):
        data_dict['demon_area_id_id'] = demon_area_id_id
        create_datetime = f"{data_dict['DATE'].strftime('%Y-%m-%d')} {str(data_dict['TIME'])}"
        date_id = int(data_dict['DATE'].strftime('%Y%m%d'))
        data_dict['create_datetime'] = create_datetime
        data_dict['date_id'] = date_id
        del data_dict['ID']
        del data_dict['DATE']
        del data_dict['TIME']
        del data_dict['풍속10분최대_ms']
        del data_dict['풍속10분평균_ms']
        
        # TR1_일사량 이름이 -> sun_1 변경
        data_dict['sun_1'] = data_dict.pop('RT1_일사량')
        data_dict['sun_2'] = data_dict.pop('RT2_일사량')
        data_dict['sun_3'] = data_dict.pop('RT3_일사량')
        data_dict['sun_4'] = data_dict.pop('RT4_일사량')
        data_dict['sun_5'] = data_dict.pop('RT5_일사량')
        data_dict['wd_n'] = data_dict.pop('풍향_N')
        data_dict['wd_t'] = data_dict.pop('풍향_T')
        data_dict['ws'] = data_dict.pop('풍속_ms')
        data_dict['ws_1minute_max'] = None
        
        DB.change_name(data_dict,'module_temp_1','RT1_모듈온도')
        DB.change_name(data_dict,'module_temp_3','RT3_모듈온도')
        DB.change_name(data_dict,'soil_temp_2','RT2_외기온도')
        DB.change_name(data_dict,'soil_temp_4','RT4_외기온도')
        DB.change_name(data_dict,'soil_temp_5','RT5_외기온도')


    def data_dict_organize_inv_100k(demon_area_id_id,device_id, data_dict):
        data_dict['demon_area_id_id'] = demon_area_id_id
        data_dict['device_id'] = device_id
        create_datetime = f"{data_dict['DATE'].strftime('%Y-%m-%d')} {str(data_dict['TIME'])}"
        date_id = int(data_dict['DATE'].strftime('%Y%m%d'))
        data_dict['create_datetime'] = create_datetime
        data_dict['date_id'] = date_id
        del data_dict['ID']
        del data_dict['DATE']
        del data_dict['TIME']
        del data_dict['TOTAL_MWh']
        del data_dict['MONTH_kWh']
        del data_dict['YEAR_MWh']
        del data_dict['state']
        del data_dict['statuscode']
        del data_dict['인버터효율']
        
        data_dict['total_1'] = data_dict.pop('TOTAL_kWh')
        data_dict['today_power_1'] = data_dict.pop('DAY_kWh')
        data_dict['power_factor_1'] = data_dict.pop('역률')
        data_dict['pac_today_max_1'] = data_dict.pop('PAC_당일최대')
        data_dict['output_power_1'] = data_dict.pop('PAC')
        data_dict['rs_volt_1'] = data_dict.pop('RS_V')
        data_dict['st_volt_1'] = data_dict.pop('ST_V')
        data_dict['tr_volt_1'] = data_dict.pop('TR_V')
        data_dict['r_current_1'] = data_dict.pop('R_A')
        data_dict['s_current_1'] = data_dict.pop('S_A')
        data_dict['t_current_1'] = data_dict.pop('T_A')
        data_dict['frequency_1'] = data_dict.pop('주파수')
        data_dict['temp_1'] = data_dict.pop('주변온도')
        data_dict['pdc_1'] = data_dict.pop('PDC')
        data_dict['r_volt_1'] = data_dict.pop('R_V')
        data_dict['s_volt_1'] = data_dict.pop('S_V')
        data_dict['t_volt_1'] = data_dict.pop('T_V')
        
        data_dict['pv_volt_1'] = data_dict.pop('PV1_V')
        data_dict['pv_volt_2'] = data_dict.pop('PV2_V')
        data_dict['pv_volt_3'] = data_dict.pop('PV3_V')
        data_dict['pv_volt_4'] = data_dict.pop('PV4_V')
        data_dict['pv_volt_5'] = data_dict.pop('PV5_V')
        data_dict['pv_volt_6'] = data_dict.pop('PV6_V')
        data_dict['pv_volt_7'] = data_dict.pop('PV7_V')
        data_dict['pv_volt_8'] = data_dict.pop('PV8_V')
        data_dict['pv_volt_9'] = data_dict.pop('PV9_V')
        
        data_dict['pv_current_1'] = data_dict.pop('PV1_A')
        data_dict['pv_current_2'] = data_dict.pop('PV2_A')
        data_dict['pv_current_3'] = data_dict.pop('PV3_A')
        data_dict['pv_current_4'] = data_dict.pop('PV4_A')
        data_dict['pv_current_5'] = data_dict.pop('PV5_A')
        data_dict['pv_current_6'] = data_dict.pop('PV6_A')
        data_dict['pv_current_7'] = data_dict.pop('PV7_A')
        data_dict['pv_current_8'] = data_dict.pop('PV8_A')
        data_dict['pv_current_9'] = data_dict.pop('PV9_A')
        
        data_dict['pv_power_1'] = data_dict.pop('PV1_kW')
        data_dict['pv_power_2'] = data_dict.pop('PV2_kW')
        data_dict['pv_power_3'] = data_dict.pop('PV3_kW')
        data_dict['pv_power_4'] = data_dict.pop('PV4_kW')
        data_dict['pv_power_5'] = data_dict.pop('PV5_kW')
        data_dict['pv_power_6'] = data_dict.pop('PV6_kW')
        data_dict['pv_power_7'] = data_dict.pop('PV7_kW')
        data_dict['pv_power_8'] = data_dict.pop('PV8_kW')
        data_dict['pv_power_9'] = data_dict.pop('PV9_kW')
        
    def data_dict_organize_inv_25k(demon_area_id_id,device_id, data_dict):
        data_dict['demon_area_id_id'] = demon_area_id_id
        data_dict['device_id'] = device_id
        create_datetime = f"{data_dict['DATE'].strftime('%Y-%m-%d')} {str(data_dict['TIME'])}"
        date_id = int(data_dict['DATE'].strftime('%Y%m%d'))
        data_dict['create_datetime'] = create_datetime
        data_dict['date_id'] = date_id
        del data_dict['ID']
        del data_dict['DATE']
        del data_dict['TIME']
        del data_dict['TOTAL_MWh']
        del data_dict['MONTH_kWh']
        del data_dict['YEAR_MWh']
        del data_dict['state']
        del data_dict['ERROR']      
        data_dict['r_current_1']        = data_dict.pop('R_A')
        data_dict['s_current_1']        = data_dict.pop('S_A')
        data_dict['t_current_1']        = data_dict.pop('T_A')
        data_dict['rs_volt_1']        = data_dict.pop('RS_V')
        data_dict['output_power_1']        = data_dict.pop('OUT_kW')
        data_dict['st_volt_1']        = data_dict.pop('ST_V')
        data_dict['tr_volt_1']        = data_dict.pop('TR_V')
        data_dict['today_power_1']        = data_dict.pop('DAY_kWh')
        data_dict['total_1']        = data_dict.pop('TOTAL_kWh')
        data_dict['temp_1']        = data_dict.pop('TEMP')
        data_dict['frequency_1']        = data_dict.pop('F')
        data_dict['pv_current_1']        = data_dict.pop('PV1_A')
        data_dict['pv_volt_1']        = data_dict.pop('PV1_V')
        data_dict['pv_power_1']        = data_dict.pop('PV1_kW')
        data_dict['pv_current_2']        = data_dict.pop('PV2_A')
        data_dict['pv_volt_2']        = data_dict.pop('PV2_V')
        data_dict['pv_current_3']        = data_dict.pop('PV3_A')
        data_dict['pv_volt_3']        = data_dict.pop('PV3_V')
        data_dict['pv_power_3']        = data_dict.pop('PV3_kW')
        data_dict['pv_current_4']        = data_dict.pop('PV4_A')
        data_dict['pv_volt_4']        = data_dict.pop('PV4_V')
        data_dict['pv_current_5']        = data_dict.pop('PV5_A')
        data_dict['pv_volt_5']        = data_dict.pop('PV5_V')
        data_dict['pv_power_5']        = data_dict.pop('PV5_kW')
        data_dict['pv_current_6']        = data_dict.pop('PV6_A')
        data_dict['pv_volt_6']        = data_dict.pop('PV6_V')
        
    def Demonstration_Area_table_Check(demon_area_id_id):
        query = f'SELECT table_name FROM tb_inv_check WHERE demon_area_id_id = {demon_area_id_id}'
        query_set = DB.selectQuery_new_rnd(query)
        table_name = query_set[0]['table_name']
        return table_name   
       
    def data_dict_organize_inv_3k(demon_area_id_id,device_id, data_dict):
        data_dict['demon_area_id_id'] = demon_area_id_id
        data_dict['device_id'] = device_id
        create_datetime = f"{data_dict['DATE'].strftime('%Y-%m-%d')} {str(data_dict['TIME'])}"
        date_id = int(data_dict['DATE'].strftime('%Y%m%d'))
        data_dict['create_datetime'] = create_datetime
        data_dict['date_id'] = date_id
        del data_dict['ID']
        del data_dict['DATE']
        del data_dict['TIME']
        del data_dict['TOTAL_MWh']
        del data_dict['MONTH_kWh']
        del data_dict['YEAR_MWh']
        del data_dict['state']
        del data_dict['ERROR']
        data_dict['pv_volt_1'] = data_dict.pop('IN_V')
        data_dict['pv_current_1'] = data_dict.pop('IN_A')
        data_dict['pv_power_1'] = data_dict.pop('IN_kW')
        data_dict['volt_1'] = data_dict.pop('OUT_V')
        data_dict['current_1'] = data_dict.pop('OUT_A')
        data_dict['output_power_1'] = data_dict.pop('OUT_kW')
        data_dict['temp_1'] = data_dict.pop('TEMP')
        data_dict['today_power_1'] = data_dict.pop('DAY_kWh')
        data_dict['total_1'] = data_dict.pop('TOTAL_kWh')
        data_dict['frequency_1'] = data_dict.pop('F')
    # 
    def db_inv_dump():
        gch_3k_1 = DB.select_tb_demonstration('n_gch_3k_1')
        for data in gch_3k_1:
            DB.data_dict_organize_inv_3k(5,14,data)
        gch_3k_2 = DB.select_tb_demonstration('n_gch_3k_2')
        for data in gch_3k_2:
            DB.data_dict_organize_inv_3k(5,15,data)
        gch_3k_3 = DB.select_tb_demonstration('n_gch_3k_3')
        for data in gch_3k_3:
            DB.data_dict_organize_inv_3k(5,16,data)
        gch_3k_4 = DB.select_tb_demonstration('n_gch_3k_4')
        for data in gch_3k_4:
            DB.data_dict_organize_inv_3k(5,17,data)
        muan_3 = DB.select_tb_demonstration('n_muan_3k_3')
        for data in muan_3:
            DB.data_dict_organize_inv_3k(1,3,data)
        muan_4 = DB.select_tb_demonstration('n_muan_3k_4')
        for data in muan_4:
            DB.data_dict_organize_inv_3k(1,4,data)
        muan_5 = DB.select_tb_demonstration('n_muan_3k_5')
        for data in muan_5:
            DB.data_dict_organize_inv_3k(1,5,data)
        muan_6 = DB.select_tb_demonstration('n_muan_3k_6')
        for data in muan_6:
            DB.data_dict_organize_inv_3k(1,6,data)
        muan_7= DB.select_tb_demonstration('n_muan_3k_7')
        for data in muan_7:
            DB.data_dict_organize_inv_3k(1,7,data)
        muan_8 = DB.select_tb_demonstration('n_muan_3k_8')
        for data in muan_8:
            DB.data_dict_organize_inv_3k(1,8,data)
        field_1 = DB.select_tb_demonstration('n_gunwi_field_100k')
        for data in field_1:
            DB.data_dict_organize_inv_100k(3,12,data)
        orchard_1 = DB.select_tb_demonstration('n_gunwi_orchard_100k')
        for data in orchard_1:
            DB.data_dict_organize_inv_100k(4,13,data)
        rice_paddy_1 = DB.select_tb_demonstration('n_gunwi_paddy_25k_1')
        for data in rice_paddy_1:
            DB.data_dict_organize_inv_25k(2,9,data)
        rice_paddy_2 = DB.select_tb_demonstration('n_gunwi_paddy_25k_2')
        for data in rice_paddy_2:
            DB.data_dict_organize_inv_25k(2,10,data)
        rice_paddy_3 = DB.select_tb_demonstration('n_gunwi_paddy_25k_3')
        for data in rice_paddy_3:
            DB.data_dict_organize_inv_25k(2,11,data)
        muan_1 = DB.select_tb_demonstration('n_muan_25k_1')
        for data in muan_1:
            DB.data_dict_organize_inv_25k(1,1,data)
        muan_2 = DB.select_tb_demonstration('n_muan_25k_2')
        for data in muan_2:
            DB.data_dict_organize_inv_25k(1,2,data)
        TB_demon_area_id_1_all_data = muan_1 + muan_2 + muan_3 + muan_4 + muan_5 + muan_6 + muan_7 + muan_8 
        TB_demon_area_id_2_all_data = rice_paddy_1 + rice_paddy_2 + rice_paddy_3
        TB_demon_area_id_3_all_data = field_1
        TB_demon_area_id_4_all_data =orchard_1
        TB_demon_area_id_5_all_data = gch_3k_1 + gch_3k_2 + gch_3k_3 + gch_3k_4
        sorted_all_data_1 = sorted(TB_demon_area_id_1_all_data, key=lambda x: datetime.strptime(x['create_datetime'], '%Y-%m-%d %H:%M:%S'))
        sorted_all_data_2 = sorted(TB_demon_area_id_2_all_data, key=lambda x: datetime.strptime(x['create_datetime'], '%Y-%m-%d %H:%M:%S'))
        sorted_all_data_3 = sorted(TB_demon_area_id_3_all_data, key=lambda x: datetime.strptime(x['create_datetime'], '%Y-%m-%d %H:%M:%S'))
        sorted_all_data_4 = sorted(TB_demon_area_id_4_all_data, key=lambda x: datetime.strptime(x['create_datetime'], '%Y-%m-%d %H:%M:%S'))
        sorted_all_data_5 = sorted(TB_demon_area_id_5_all_data, key=lambda x: datetime.strptime(x['create_datetime'], '%Y-%m-%d %H:%M:%S'))
        DB.key_setting(sorted_all_data_1)
        
        
        
        
        
        
        # for data in demon_area_1_data_set:
        #     DB.data_dict_organize(1,data)
    def db_env_dump():
        demon_area_1_data_set = DB.select_tb_demonstration(TB_demon_area_id_1)
        for data in demon_area_1_data_set:
            DB.data_dict_organize(1,data)
        demon_area_2_data_set = DB.select_tb_demonstration(TB_demon_area_id_2)
        for data in demon_area_2_data_set:
            DB.data_dict_organize(2,data)
        demon_area_3_data_set = DB.select_tb_demonstration(TB_demon_area_id_3)
        for data in demon_area_3_data_set:
            DB.data_dict_organize(3,data)
        demon_area_4_data_set = DB.select_tb_demonstration(TB_demon_area_id_4)
        for data in demon_area_4_data_set:
            DB.data_dict_organize(4,data)
        demon_area_5_data_set = DB.select_tb_demonstration(TB_demon_area_id_5)
        for data in demon_area_5_data_set:
            DB.data_dict_organize(5,data)
        all_data = demon_area_1_data_set + demon_area_2_data_set + demon_area_3_data_set + demon_area_4_data_set + demon_area_5_data_set
        sorted_all_data = sorted(all_data, key=lambda x: datetime.strptime(x['create_datetime'], '%Y-%m-%d %H:%M:%S'))
        DB.data_db_save('tb_env_data',sorted_all_data)
# 메인 함수
if __name__ == "__main__":
    print('main')
    # DB.db_env_dump()
    DB.db_inv_dump()