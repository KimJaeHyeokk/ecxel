from apscheduler.schedulers.background import BackgroundScheduler
import urllib
import urllib.request
import os
import time
import json
from datetime import datetime, timedelta
import time
import pymysql

class QuerySet():
    def selectQuery(self, query, distinct=None):
        connection = pymysql.connect(
            host='1.223.240.125',
            port=3306,
            user='grandsun',
            password='grandsun!@',
            db='new_rnd'
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