# Name: Rohith Reddy Gurram
# ID: 1001959477
# Course: CSE 6331
# Programming Assignment 3

import json
import random
from flask import Flask, render_template, request, url_for
import pypyodbc as odbc
import time 
import redis

app = Flask(__name__)

# Connection details for SQL Server
connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:freecloudsqlserver001.database.windows.net,1433;Database=DemoSQLServerDB;Uid=RohithGurram;Pwd={freecloudsqlserver@123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
conn = odbc.connect(connection_string)

server = '10.207.170.226'
portno = 6379
# Connection details for Redis
redis_cache = redis.StrictRedis(host=server, port=portno, decode_responses=True)


@app.route('/', methods =["GET", "POST"])
def home():
    if request.method == "GET":
        return render_template("home.html")


@app.route('/randomquery.html', methods =["GET", "POST"])
def randomquery():
    if request.method == "POST":
        timer = []
        num = request.form.get("num")

        # start time for calculating total time
        start_time1 = time.time()
        for i in range(int(num)):
            # generate random values for the query
            lat = random.uniform(-90, 90)
            long = random.uniform(-180,180)
            depth = random.uniform(1, 20)
            
            # build the query
            sql_stmt = "SELECT * FROM dbo.earthquake_data WHERE latitude = ? AND longitude = ? AND depth = ?"
            values = (lat, long, depth)
            
            # start time for calculating single transaction time
            start_time2 = time.time()
            
            # execute the query
            cursor = conn.cursor()
            cursor.execute(sql_stmt, values)
            results = cursor.fetchall()

            # end time for calculating single transaction time
            end_time2 = time.time()
            exec_time = end_time2-start_time2
            
            # Saving transaction times into a list
            timer.append(exec_time)

        # end time for calculating total time
        end_time1 = time.time()
        total_time = end_time1- start_time1
        return render_template("randomquery.html", total_time=total_time, timer=timer)


@app.route('/restrictedquery.html', methods =["GET", "POST"])
def restrictedquery():
    if request.method == "POST":
        # start time for calculating total time
        start_time2 = time.time()
        mag1 = request.form.get("mag1")
        mag2 = request.form.get("mag2")
        num = request.form.get("num")

        timetaken =  []
        
        for i in range(int(num)):
            start_time = time.time()
            sql = '''SELECT latitude, longitude, depth, mag, id, place, type FROM dbo.earthquake_data 
                WHERE mag BETWEEN ? AND ?'''
            values = (mag1, mag2)

            #Fetching data from database
            cursor = conn.cursor()
            cursor.execute(sql, values)
            results = cursor.fetchall()

            # end time for calculating single transaction time
            end_time = time.time()
            total_time = end_time - start_time
            timetaken.append(total_time)
        
        # end time for calculating total time
        end_time2 = time.time()
        totality_time = end_time2 - start_time2
        
        return render_template("restrictedquery.html", timetaken = timetaken, data = results, totality_time = totality_time)


@app.route('/randomquery_redis.html', methods =["GET", "POST"])
def randomquery_redis():
    if request.method == "POST":
        timer = []
        num = request.form.get("num")
        # start time for calculating total time
        start_time1 = time.time()
        for i in range(int(num)):
            
            # generate random values for the query
            lat = random.uniform(-90, 90)
            long = random.uniform(-180,180)
            depth = random.uniform(1, 20)
            
            sql = "SELECT * FROM dbo.earthquake_data WHERE latitude = ? AND longitude = ?"
            values = (lat, long)
            my_string = json.dumps(values)
            sql_key = my_string

            # start time for calculating single transaction time
            start_time2 = time.time()

            # Check whether the data exists in Redis or not
            if redis_cache.exists(sql_key):
                results = redis_cache.get(json.loads(sql_key))
            else:
                # Fetching data from database
                cursor = conn.cursor()
                cursor.execute(sql, values)
                results = cursor.fetchall()

                # Storing the data in Redis 
                s_data = json.dumps(results)
                redis_cache.set(sql_key, s_data)
            
            # end time for calculating single transaction time
            end_time2 = time.time()
            time1 = end_time2 - start_time2

            # Saving each transaction time in a list
            timer.append(time1)

        # end time for calculating total time
        end_time1 = time.time()
        total_time = end_time1 - start_time1
        return render_template("randomquery_redis.html", time=total_time, timer=timer)


@app.route('/restrictedquery_redis.html', methods =["GET", "POST"])
def restrictedquery_redis():
    if request.method == "POST":
        # start time for calculating total time
        start_time1 = time.time()
        mag1 = request.form.get("mag1")
        mag2 = request.form.get("mag2")
        num = request.form.get("num")

        timetaken =  []
        
        for i in range(int(num)):
            
            sql = '''SELECT latitude, longitude, depth, mag, id, place, type FROM dbo.earthquake_data 
                WHERE mag BETWEEN ? AND ?'''
            values = (mag1, mag2)
            my_string = json.dumps(values)
            sql_key = my_string
            # start time for calculating single transaction time
            start_time = time.time()

            # Check whether the data exists in Redis or not
            if redis_cache.exists(sql_key):
                results = redis_cache.get(sql_key)
            else:
                # Fetching the data from Database
                cursor = conn.cursor()
                cursor.execute(sql, values)
                results = cursor.fetchall()

                # Storing the data in Redis
                s_data = json.dumps(results)
                redis_cache.set(sql_key, s_data)

            # end time for calculating single transaction time
            end_time = time.time()
            total_time = end_time - start_time

            # Saving each transaction time in a list
            timetaken.append(total_time)

        # end time for calculating total time
        end_time1 = time.time()
        total_time = end_time1 - start_time1
        
        cursor = conn.cursor()
        cursor.execute(sql, values)
        results = cursor.fetchall()

        return render_template("restrictedquery_redis.html", timetaken = timetaken, data = results, total_time = total_time)
        

if __name__ == "__main__":
    app.run(debug=True)