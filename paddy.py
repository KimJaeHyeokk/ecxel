import pymysql

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
                
                try:
                    cursor.execute(query)
                    conn.commit()  # Ensure to commit changes
                except Exception as e:
                    print("Error:", e)
                    print("Query execution failed.")


# 메인 함수
if __name__ == "__main__":
    print('START Scheduler')
    values_1 = 
                [72.9,
                68.0,
                23.9,
                84.7,
                75.8,
                17.9,
                40.4,
                41.6,
                15.5,
                39.6,
                44.7,
                35.9,
                23.3,
                50.9,
                83.9,
                75.6,
                70.1,
                90.3,
                95.7,
                77.0,
                81.7,
                67.1,
                75.4,
                58.4,
                69.4,
                56.8,
                69.3,
                69.1,
                76.4,
                75.5,
                47.9
                ]
    values_2 = [
        80.2,
        93.1,
        73.2,
        86.3,
        77.1,
        69.4,
        82.1,
        71.4,
        78.9,
        84.9,
        43.3,
        43.5,
        86.0,
        82.5,
        79.5,
        78.3,
        79.1,
        91.2,
        83.1,
        74.0,
        43.2,
        60.8,
        68.7,
        68.3,
        58.3,
        65.4,
        20.9,
        36.7,
        25.7,
        94.8,
        75.6,
    ]
    date_ids_1 = [
        20220701,
        20220702,
        20220703,
        20220704,
        20220705,
        20220706,
        20220707,
        20220708,
        20220709,
        20220710,
        20220711,
        20220712,
        20220713,
        20220714,
        20220715,
        20220716,
        20220717,
        20220718,
        20220719,
        20220720,
        20220721,
        20220722,
        20220723,
        20220724,
        20220725,
        20220726,
        20220727,
        20220728,
        20220729,
        20220730,
        20220731,
    ]
    date_ids_2 = [
        20220801,
        20220802,
        20220803,
        20220804,
        20220805,
        20220806,
        20220807,
        20220808,
        20220809,
        20220810,
        20220811,
        20220812,
        20220813,
        20220814,
        20220815,
        20220816,
        20220817,
        20220818,
        20220819,
        20220820,
        20220821,
        20220822,
        20220823,
        20220824,
        20220825,
        20220826,
        20220827,
        20220828,
        20220829,
        20220830,
        20220831,
    ]
    values = values_1 + values_2
    date_ids = date_ids_1 + date_ids_2
    # values = [  2076.8,	1874.9,	2155.4,	1911.2,	1804.1,	1055.4,	991.0,	1240.9,	1364.3,	1900.9,	2008.8,	2214.1,
    #             2060.2,	1859.9,	2138.1,	1895.9,	1789.6,	1047.0,	983.1,	1231.0,	1401.7,	1885.7,	1992.8,	2196.4,
    #             2043.7,	1845.0,	2121.0,	1880.8,	1775.3,	1038.6,	975.2,	1221.2,	1342.6,	1870.6,	1976.8,	2178.8,
    #             2027.4,	1830.2,	2104.0,	1865.7,	1761.1,	1030.3,	967.4,	1211.4,	1331.9,	1855.6,	1961.0,	2161.4,
    #             2011.1,	1815.6,	2087.2,	1850.8,	1747.0,	1022.1,	959.7,	1201.7,	1321.2,	1840.8,	1945.3,	2144.1,
    #             1995.0,	1801.1,	2070.5,	1836.0,	1733.0,	1013.9,	952.0,	1192.1,	1357.4,	1826.1,	1929.7,	2127.0,
    #             1979.1,	1786.7,	2053.9,	1821.3,	1719.2,	1005.8,	944.4,	1182.6,	1300.1,	1811.5,	1914.3,	2109.9,
    #             1963.3,	1772.4,	2037.5,	1806.7,	1705.4,	997.7,  936.8,	1173.1,	1289.7,	1797.0,	1899.0,	2093.1,
    #             1947.5,	1758.2,	2021.2,	1792.3,	1691.8,	989.7,  929.3,	1163.7,	1279.4,	1782.6,	1883.8,	2076.3,
    #             1932.0,	1744.1,	2005.0,	1777.9,	1678.2,	981.8,  921.9,	1154.4,	1314.5,	1768.3,	1868.7,	2059.7,
    #             1916.5,	1730.2,	1989.0,	1763.7,	1664.8,	974.0,  914.5,	1145.2,	1259.0,	1754.2,	1853.8,	2043.2,
    #             1901.2,	1716.3,	1973.1,	1749.6,	1651.5,	966.2,  907.2,	1136.0,	1249.0,	1740.2,	1839.0,	2026.9,
    #             1886.0,	1702.6,	1957.3,	1735.6,	1638.3,	958.4,  900.0,	1126.9,	1239.0,	1726.2,	1824.2,	2010.7,
    #             1870.9,	1689.0,	1941.6,	1721.7,	1625.2,	950.8,  892.8,	1117.9,	1273.0,	1712.4,	1809.6,	1994.6,
    #             1855.9,	1675.5,	1926.1,	1707.9,	1612.2,	943.2,  885.6,	1109.0,	1219.2,	1698.7,	1795.2,	1978.6,
    #             1841.1,	1662.1,	1910.7,	1694.3,	1599.3,	935.6,  878.5,	1100.1,	1209.5,	1685.1,	1780.8,	1962.8,
    #             1826.3,	1648.8,	1895.4,	1680.7,	1586.5,	928.1,  871.5,	1091.3,	1199.8,	1671.7,	1766.6,	1947.1,
    #             1811.7,	1635.6,	1880.3,	1667.3,	1573.8,	920.7,  864.5,	1082.6,	1208.1,	1658.3,	1752.4,	1931.5,
    #             1797.2,	1622.5,	1865.2,	1653.9,	1561.2,	913.3,  857.6,	1073.9,	1198.5,	1645.0,	1738.4,	1916.1,
    #             1782.9,	1609.5,	1850.3,	1640.7,	1548.7,	906.0,  850.7,	1065.3,	1188.9,	1631.9,	1724.5,	1900.7
    # ]

    # date_id_id = [  202206,202207,202208,202209,202210,202211,202212,202301,202302,202303,202304,202305,     #2022
    #                 202306,202307,202308,202309,202310,202311,202312,202401,202402,202403,202404,202405,     #2023
    #                 202406,202407,202408,202409,202410,202411,202412,202501,202502,202503,202504,202505,     #2024
    #                 202506,202507,202508,202509,202510,202511,202512,202601,202602,202603,202604,202605,     #2025
    #                 202606,202607,202608,202609,202610,202611,202612,202701,202702,202703,202704,202705,     #2026
    #                 202706,202707,202708,202709,202710,202711,202712,202801,202802,202803,202804,202805,     #2027
    #                 202806,202807,202808,202809,202810,202811,202812,202901,202902,202903,202904,202905,     #2028
    #                 202906,202907,202908,202909,202910,202911,202912,203001,203002,203003,203004,203005,     #2029
    #                 203006,203007,203008,203009,203010,203011,203012,203101,203102,203103,203104,203105,     #2030
    #                 203106,203107,203108,203109,203110,203111,203112,203201,203202,203203,203204,203205,     #2031
    #                 203206,203207,203208,203209,203210,203211,203212,203201,203302,203303,203304,203305,     #2032
    #                 203306,203307,203308,203309,203310,203311,203312,203401,203402,203403,203404,203405,     #2033
    #                 203406,203407,203408,203409,203410,203411,203412,203501,203502,203503,203504,203505,     #2034
    #                 203506,203507,203508,203509,203510,203511,203512,203601,203602,203603,203604,203605,     #2035
    #                 203606,203607,203608,203609,203610,203611,203612,203701,203702,203703,203704,203705,     #2036
    #                 203706,203707,203708,203709,203710,203711,203712,203801,203802,203803,203804,203805,     #2037
    #                 203806,203807,203808,203809,203810,203811,203812,203901,203902,203903,203904,203905,     #2038
    #                 203906,203907,203908,203909,203910,203911,203912,204001,204002,204003,204004,204005,     #2039
    #                 204006,204007,204008,204009,204010,204011,204012,204101,204102,204103,204104,204105,     #2040
    #                 204106,241107,204108,204109,204110,204111,204112,204201,204202,204203,204204,204205      #2041
    # ]


        
    device_id = 8
    test(values,date_id_id,device_id)