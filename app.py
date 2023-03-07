import random
from flask import Flask, render_template, request, url_for
import pypyodbc as odbc
import time 
import redis
import pickle

app = Flask(__name__)

server = 'cloudsqlserver01.database.windows.net'
database = 'DemoSQLServerDB'
connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:freecloudsqlserver001.database.windows.net,1433;Database=DemoSQLServerDB;Uid=RohithGurram;Pwd={freecloudsqlserver@123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
conn = odbc.connect(connection_string)

redis_client = redis.Redis(host='localhost', port=6379, db=0)

def create_redis_client():
    redis_client = redis.Redis(host='localhost', port=6379, db=0)
    return redis_client


@app.route('/', methods =["GET", "POST"])
def home():
    if request.method == "GET":
        return render_template("home.html")
    if request.method == "POST":
        cur = conn.cursor()
        sql1 = "SELECT * FROM dbo.earthquake_data"
        cur.execute(sql1)
        data = cur.fetchall()

        cur.close()

        return render_template("all_earthquake_data.html", data=data)

@app.route('/time_1000query.html', methods =["GET", "POST"])
def randomquery():
    if request.method == "POST":
        start_time = time.time()
        for i in range(1000):
            # generate random values for the query
            lat = random.uniform(-90, 90)
            long = random.uniform(-180,180)
            depth = random.uniform(1, 20)
            
            # build the query
            sql1 = "SELECT * FROM dbo.earthquake_data"
            values = (lat, long, depth)
            
            # execute the query
            cursor = conn.cursor()
            cursor.execute(sql1)
            results = cursor.fetchall()

        end_time = time.time()
        total_time = end_time - start_time
        return render_template("time_1000query.html", time=total_time)

@app.route('/filtered_query.html', methods =["GET", "POST"])
def filteredquery():
    if request.method == "POST":
        start_time = time.time()
        rank1 = request.form.get("rk1")
        rank2 = request.form.get("rk2")
        state = request.form.get("st")
        
        sql = "SELECT city, state, rank, population FROM dbo.cities WHERE state = ? AND rank BETWEEN ? AND ?"
        values = (state, rank1, rank2)

        cursor = conn.cursor()
        cursor.execute(sql, values)
        results = cursor.fetchall()

        end_time = time.time()

        total_time = end_time - start_time
        return render_template("filtered_query.html", time=total_time, data = results)
    
@app.route('/filtered_queryq11.html', methods =["GET", "POST"])
def filteredqueryq11():
    if request.method == "POST":
        start_time = time.time()
        rank1 = request.form.get("rk1")
        rank2 = request.form.get("rk2")
        state = request.form.get("st")
        num = request.form.get("num")

        timetaken =  []
        
        for i in range(int(num)):
            start_time = time.time()
            sql = "SELECT city, state, rank, population FROM dbo.cities WHERE state = ? AND rank BETWEEN ? AND ?"
            values = (state, rank1, rank2)

            cursor = conn.cursor()
            cursor.execute(sql, values)
            results = cursor.fetchall()
            end_time = time.time()
            total_time = end_time - start_time
            timetaken.append(total_time)
        
        return render_template("filtered_queryq11.html", timetaken = timetaken, data = results)

@app.route('/redisq12.html', methods =["GET", "POST"])
def redisq12():
    if request.method == "POST":
        start_time = time.time()
        for i in range(1000):
            
            # generate random values for the query
            lat = random.uniform(-90, 90)
            long = random.uniform(-180,180)
            depth = random.uniform(1, 20)
            
            # build the query
            # sql1 = "SELECT * FROM dbo.earthquake_data WHERE latitude = ? AND longitude = ? AND depth = ?"
            # values = (lat, long, depth)

            cursor = conn.cursor()
            sql = "SELECT * FROM dbo.earthquake_data WHERE lat = ? and long = ?"
            values = (lat, long)
            my_string = json.dumps(values)
            sql_key = my_string

            if redis_client.exists(sql_key):
                results = pickle.loads(redis_client.get(sql_key))
            else:
                cursor = conn.cursor()
                cursor.execute(sql, values)
                columns = [column[0] for column in cursor.description]
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                redis_client.set(sql_key, pickle.dumps(rows), ex=3600)

        end_time = time.time()
        total_time = end_time - start_time
        return render_template("time_1000query_redis.html", time=total_time)


@app.route('/time_1000query_redis.html', methods =["GET", "POST"])
def redis():
    if request.method == "POST":
        start_time = time.time()
        for i in range(1000):
            
            # generate random values for the query
            lat = random.uniform(-90, 90)
            long = random.uniform(-180,180)
            depth = random.uniform(1, 20)
            
            # build the query
            # sql1 = "SELECT * FROM dbo.earthquake_data WHERE latitude = ? AND longitude = ? AND depth = ?"
            # values = (lat, long, depth)

            cursor = conn.cursor()
            sql = "SELECT * FROM dbo.earthquake_data WHERE lat = ? and long = ?"
            values = (lat, long)
            my_string = json.dumps(values)
            sql_key = my_string

            if redis_client.exists(sql_key):
                results = pickle.loads(redis_client.get(sql_key))
            else:
                cursor = conn.cursor()
                cursor.execute(sql, values)
                columns = [column[0] for column in cursor.description]
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                redis_client.set(sql_key, pickle.dumps(rows), ex=3600)

        end_time = time.time()
        total_time = end_time - start_time
        return render_template("time_1000query_redis.html", time=total_time)
    



if __name__ == "__main__":
    app.run(debug=True)