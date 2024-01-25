from pymodbus.client.sync import ModbusTcpClient
from pymodbus.transaction import ModbusRtuFramer as ModbusFramer
from pymodbus.exceptions import ModbusIOException
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
import time
import json

# 3초안에 모든게 다 돌아야함
# 실패한 경우도 생각해야하니까 한줄씩 읽게 하면 계속 밀릴거 각 셋팅된 ip port별로 각각의 스레드 돌림? 이건 OK
# IP전체 갯수 5개 PROT도 5개 포트는 각각의 역할이 있음
# 셋팅값들을 JSON파일에 저장하면 가독성은 좋아질거임
# 포트 4001은 모두가 환경센서
# 인버터 3.5kW짜리, 25kW짜리, 100kW짜리 분류해야 할듯함
# 전역 변수 셋팅
####################################################전체 전역 변수 셋팅(ip,port,address,get_count)##################################
#IP 셋팅
RICE_PADDY_SERVER_IP = '223.171.136.17' #논
FIELD_SERVER_IP = '223.171.136.18'      # 밭
ORCHARD_SERVER_IP = '223.171.136.19'    #과수원
GEOCHANG_SERVER_IP = '223.171.136.36'   #거창
SALTERN_SERVER_IP = '223.171.136.86'    #염전
ALL_SERVER_IP = [RICE_PADDY_SERVER_IP,FIELD_SERVER_IP,ORCHARD_SERVER_IP,GEOCHANG_SERVER_IP,SALTERN_SERVER_IP]
#SLAVE ID 셋팅
SLAVE_ID_1_6 = [1,2,3,4,5,6]   #기상센서 (1~4) 수평 (5) 풍속 (6)    #100kW인버터 (1)
SLAVE_ID_RICE_PADDY = [19,20,21]    #인버터 논 4002         # 25kW
SLAVE_ID_GEOCHANG = [28,92,96,99]   #인버터 거창 4002       # 3.5kW
SLAVE_ID_SALTERN_25kW = [61,55]     # 포트상으로는 [4003, 4004] 25kw
SLAVE_ID_SALTERN_3kW = [24,73,97,29,32,37] # 포트상으로는 4005  3kw
#PORT 셋팅
SERVER_PORT_4001 = 4001 #기상센서 포트
SERVER_PORT_4002 = 4002 # 논,밭,과수원, 거창 인버터 포트
SERVER_PORT_4003 = 4003 # 염전 25KW     slaveid = 61
SERVER_PORT_4004 = 4004 # 염전 25KW     slaveid = 55
SERVER_PORT_4005 = 4005 # 염전 3.5KW 6대
#100kW 인버터 셋팅
SETTING_100kW = {'start_address':[22,33282,1006],'get_count': [48,60,6]}
# Env 셋팅
SETTING_ENV = {'start_address':[5,22],'get_count': [10,46]}
#25kW 인버터 셋팅

#3.5kW 인버터 셋팅
# 풍향 text 셋팅
directions = ["북", "북동", "동", "남동", "남", "남서", "서", "북서"]
start_angles = [0, 45, 90, 135, 180, 225, 270, 315]
end_angles = [22.5, 67.5, 112.5, 157.5, 202.5, 247.5, 292.5, 337.5]
###############################################################################################################################
class Modbus():
    def read_holding_registers_modbus(client,address,num,slave_id):
        try:
            # 값 받아오는 부분
            response = client.read_holding_registers(address, num, unit=slave_id)
            # 못받았을때 예외 처리 None으로 값을 넘김
            if response.isError():
                # 에러로그 저장할 쿼리 진행 예정
                print(f"Modbus Error for Register {address}: {response}")
                return None
            else:
                data = response.registers
                print(f"Read data for Register {address}: {data}")
                return data
        except ModbusIOException as e:
            # 에러로그 저장할 쿼리 진행 예정
            
            print(f"Modbus Communication Error for Register {address}: {e}")
            return None
    def read_input_registers_modbus(client,address,num,slave_id):
        try:
            # 값 받아오는 부분
            response = client.read_input_registers(address, num, unit=slave_id)
            # 못받았을때 예외 처리 None으로 값을 넘김
            if response.isError():
                # 에러로그 저장할 쿼리 진행 예정
                print(f"Modbus Error for Register {address}: {response}")
                return None
            else:
                data = response.registers
                print(f"Read data for Register {address}: {data}")
                return data
        except ModbusIOException as e:
            # 에러로그 저장할 쿼리 진행 예정
            
            print(f"Modbus Communication Error for Register {address}: {e}")
            return None

class Service():
    def calculate_8_wind_direction(wind_direction): # 8방위 기준으로 계산
        # 주어진 풍향값이 어느 범위에 속하는지 확인
        for i in range(len(directions)):
            if start_angles[i] <= wind_direction < end_angles[i]:
                return directions[i]
        # 360도일 때 북으로 처리
        if wind_direction == 360:
            return "북"

        # 범위에 속하지 않을 경우 예외 처리
        return "알 수 없음"
    def Env_Modbus(ip,port,reg_settings,slave_id_list):
        client = ModbusTcpClient(ip, port=port, framer=ModbusFramer)
        try:
            client.connect()
            address_setting = reg_settings['start_address']
            get_count_setting = reg_settings['get_count']
            data_dic = {}
            for slave_id in slave_id_list:
                print("slave_id",slave_id)
                if slave_id != 6:
                    address = address_setting[0]
                    get_count = get_count_setting[0]
                else:
                    address = address_setting[1]
                    get_count = get_count_setting[1]
                data_set = Modbus.read_input_registers_modbus(client, address, get_count,slave_id)
                if data_set is not None:
                    if slave_id != 6:
                        data_dic[f'rt_{slave_id}'] = data_set[0]
                        if slave_id == 1:
                            data_dic[f'rt_{slave_id}_module_temp'] = round((data_set[-1] - 27315) / 100, 2)
                        elif slave_id == 2:
                            data_dic[f'rt_{slave_id}_soil_temp'] = round((data_set[-1] - 27315) / 100, 2)
                    elif slave_id == 6:
                        ws = round(data_set[-1] * 0.1,1)
                        wd_n = round(data_set[0] * 0.1 ,1)
                        data_dic['wd_n'] = wd_n
                        data_dic['ws'] = ws
                        wd_t =Service.calculate_8_wind_direction(wd_n)
                        data_dic['wd_t'] = wd_t
                        json_key_name = 'WS_SAVE_'
                        ip_save = ip[-2:]   # json파일에서 ws 저장 하는 용도로 쓸것
                        json_name = json_key_name + str(ip_save)
                        with open('grandsun_ws_save.json') as f:
                            json_data = json.load(f)
                        if json_data[json_name] < ws:
                            json_data[json_name] = ws
                            with open('grandsun_ws_save.json','w',encoding='utf-8') as save_file:
                                json.dump(json_data,save_file,indent='\t')
                else:
                    if slave_id != 6:
                        data_dic[f'rt_{slave_id}'] = None
                        if slave_id == 1:
                            data_dic[f'rt_{slave_id}_module_temp'] = None
                        elif slave_id == 2:
                            data_dic[f'rt_{slave_id}_soil_temp'] = None
                    elif slave_id == 6:
                        data_dic['wd_n'] = None
                        data_dic['ws'] = None
                        data_dic['wd_t'] = None
                    # 에러가 발생한 경우 또는 데이터를 읽어오지 못한 경우 어떻게 처리 해야할지 결정해야 할듯 함
                    # 에러로그 저장할 쿼리 진행 예정
                    print("Failed to receive data.")
            print('#################################################################################')
            print(f"{ip}({port}) 모든 데이터 : {data_dic}")
            print('#################################################################################')       
        except Exception as e:
            # 에러로그 저장할 쿼리 진행 예정
            print(f"Service Error: {e}")
        finally:
            # 클라이언트를 닫음
            client.close()
    # ip,port, 셋팅(주소,가져올 갯수)
    def Inv_Modbus_100kW(ip,port,reg_settings,slave_id):
        # 클라이언트 연결
        client = ModbusTcpClient(ip, port=port, framer=ModbusFramer)
        try:
            client.connect()
            # 주소값
            address_setting = reg_settings['start_address']
            # 가져올 갯수
            get_count_setting = reg_settings['get_count']
            # 만약 주소가 한개라 해도 반복문은 이상없음 현재까지는 
            # 주소 인덱스랑 가져올 갯수 인덱스가 같아서 이렇게 처리함 다를 경우 조건 추가 할 예정
            data_dic = {}
            for index in range(len(address_setting)):
                print('-----------------------------------------------------------------------------')
                address = address_setting[index]
                get_count = get_count_setting[index]
                if index == 0 or index == 1:
                    data_set = Modbus.read_input_registers_modbus(client, address, get_count,slave_id)
                else:
                    data_set = Modbus.read_holding_registers_modbus(client, address, get_count,slave_id)
                if data_set is not None:
                    if index == 0:
                        data = [value - (1 << 16) if value & (1 << 15) else value for value in data_set]
                        data_dic['total_power'] = (data[0] << 16) | data[1]
                        data_dic['today_power'] = round(0.1 * data[2], 2)
                        data_dic['power_factor'] = 0.1 * data[4]
                        data_dic['pac_max_daily'] = round(0.1 * data[5],2)
                        data_dic['pac'] = round(0.1 * data[7], 2)
                        data_dic['rs_volt'] = round(0.1 * data[9], 2)
                        data_dic['st_volt'] = round(0.1 * data[10], 2)
                        data_dic['tr_volt'] = round(0.1 * data[11], 2)
                        data_dic['r_current'] = round(0.1 * data[12], 2)
                        data_dic['s_current'] = round(0.1 * data[13], 2)
                        data_dic['t_current'] = round(0.1 * data[14], 2)
                        data_dic['frequency'] = data[21]
                        data_dic['surrounding_temperature'] = round(0.1 * data[23], 2)
                        data_dic['statusCode'] = data[25]
                        data_dic['pdc'] = round(0.1 * data[-1], 2)
                    elif index == 1:    #pv_volt,pv_current값들
                        result = data_set[:6] + data_set[48:]
                        for i in range(1, int(len(result) / 2) + 1):
                            pv_volt = result[((i-1) * 2)] * 0.1
                            pv_current = result[((i-1) * 2) + 1] * 0.1
                            pv_power = pv_volt * pv_current
                            data_dic[f'pv_volt_{i}'] = round(pv_volt, 3)
                            data_dic[f'pv_current_{i}'] = round(pv_current, 3)
                            data_dic[f'pv_power_{i}'] = round(pv_power/1000,5)
                    elif index == 2:
                        data = data_set
                        print(f"Read data: {data}")
                        data_dic['r_volt'] = round(0.1 * data[1], 2)
                        data_dic['s_volt'] = round(0.1 * data[3], 2)
                        data_dic['t_volt'] = round(0.1 * data[5], 2)
                else:
                    if index == 0:
                        data_dic['total_power'] = None
                        data_dic['today_power'] = None
                        data_dic['power_factor'] = None
                        data_dic['pac_max_daily'] =None
                        data_dic['pac'] = None
                        data_dic['rs_volt'] = None
                        data_dic['st_volt'] = None
                        data_dic['tr_volt'] = None
                        data_dic['r_current'] = None
                        data_dic['s_current'] = None
                        data_dic['t_current'] = None
                        data_dic['frequency'] = None
                        data_dic['surrounding_temperature'] = None
                        data_dic['statusCode'] = None
                        data_dic['pdc'] = None
                    elif index == 1:    #pv_volt,pv_current값들
                        for i in range(1, 9):
                            data_dic[f'pv_volt_{i}'] = None
                            data_dic[f'pv_current_{i}'] = None
                            data_dic[f'pv_power_{i}'] = None
                    elif index == 2:
                        data_dic['r_volt'] = None
                        data_dic['s_volt'] = None
                        data_dic['t_volt'] = None
                    # 에러가 발생한 경우 또는 데이터를 읽어오지 못한 경우 어떻게 처리 해야할지 결정해야 할듯 함
                    # 에러로그 저장할 쿼리 진행 예정
                    print("Failed to receive data.")
            print('#################################################################################')
            print(f"{ip}({port}) 모든 데이터 : {data_dic}")
            print('#################################################################################')
            # 이제 모든 키값들을 tb_data_info 의 이름들과 매칭시켜 db에 데이터 저장할 예정
        except Exception as e:
            # 에러로그 저장할 쿼리 진행 예정
            print(f"Service Error: {e}")
        finally:
            # 클라이언트를 닫음
            client.close()
            
class Schedulerer:
    def backgroundScheduler():
        sched = BackgroundScheduler()
        sched.start()
        for server_ip in ALL_SERVER_IP:     #환경 데이터 스케줄러
            sched.add_job(Service.Env_Modbus, 'interval', seconds=5,misfire_grace_time = 1,args=(server_ip,SERVER_PORT_4001,SETTING_ENV,SLAVE_ID_1_6))     #
        # 100kW 인버터 스케줄러
        # sched.add_job(Service.Inv_Modbus_100kW, 'interval', seconds=5,misfire_grace_time = 1,args=(ORCHARD_SERVER_IP,SERVER_PORT_4002,SETTING_100kW,SLAVE_ID_1_6[0]))     #
        # sched.add_job(Service.Inv_Modbus_100kW, 'interval', seconds=5,misfire_grace_time = 1,args=(FIELD_SERVER_IP,SERVER_PORT_4002,SETTING_100kW,SLAVE_ID_1_6[0]))    #
        
        # # sched.add_job(DB.modbus_device_open, 'cron', hour='9', minute='14', misfire_grace_time = 50, id = 'device_open') #서버 CLASS Control 이상으로 인해 임시로 여기서 open
        # # sched.add_job(DB.modbus_device_close, 'cron', hour='9', minute='24', misfire_grace_time = 50, id = 'device_close') #서버 CLASS Control 이상으로 인해 임시로 여기서 close
        # sched.add_job(DB.inverter_main, 'cron', minute='*/2', misfire_grace_time = 50, id = 'inverter') #inverter data db에 저장, inverter state 상태 db에 저장
        # sched.add_job(DB.sensor_main, 'cron', minute='*/10', misfire_grace_time = 300, id = 'sensor') #sensor data db에 저장
        # sched.add_job(DB.weather_API, 'cron', minute='*/10', misfire_grace_time = 300, id = 'weather') #weather data db에 저장
        
        try:
            while True:
                time.sleep(1)  # 메인 스레드가 종료되지 않도록 유지
        except (KeyboardInterrupt, SystemExit):
            # Ctrl+C 또는 시스템 종료 시 스케줄러 정지
            sched.shutdown()
# 메인 함수
if __name__ == "__main__":
    print('START Scheduler')
    Schedulerer.backgroundScheduler()
    # 과수원 100kW 인버터
    # Service.Env_Modbus(ALL_SERVER_IP,SERVER_PORT_4001,SETTING_ENV,SLAVE_ID_1_6)
    # Service.Inv_Modbus_100kW(ORCHARD_SERVER_IP,SERVER_PORT_4002,SETTING_100kW,SLAVE_ID_1_6[0])
    # 밭 100kw 인버터
    # Service.Inv_Modbus_100kW(FIELD_SERVER_IP,SERVER_PORT_4002,SETTING_100kW,SLAVE_ID_1_6[0])
    