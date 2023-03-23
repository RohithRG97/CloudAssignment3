import json
import random
from flask import Flask, render_template, request, url_for, jsonify
import pypyodbc as odbc
import plotly.graph_objs as go

app = Flask(__name__)

connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:freecloudsqlserver001.database.windows.net,1433;Database=DemoSQLServerDB;Uid=RohithGurram;Pwd={freecloudsqlserver@123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
conn = odbc.connect(connection_string)

@app.route('/', methods =["GET", "POST"])
def home():
    if request.method == "GET":
        return render_template("home.html")

@app.route('/scatterpage.html', methods =["GET", "POST"])
def scatterpage():
    # Replace this with your data retrieval code
    sql_stmt1 = "SELECT TOP 10 depth, mag FROM dbo.earthquake_data"
    
    
    cursor = conn.cursor()
    cursor.execute(sql_stmt1)
    result = cursor.fetchall()
    
    #data = [{'x': row[0], 'y': row[1]} for row in data]
    data = {'x': [], 'y': []}
    for row in result:
        data['x'].append(row[0])
        data['y'].append(row[1])
    json_data = json.dumps(data)

    conn.close()
    
    return render_template('scatterpage.html', data=json_data)

@app.route('/piechart1', methods =["GET", "POST"])
def piechart1():
    sql_stmt1 = "SELECT COUNT(*) FROM dbo.earthquake_data WHERE mag < 1"
    sql_stmt2 = "SELECT COUNT(*) FROM dbo.earthquake_data WHERE mag >= 1 AND mag < 2"
    sql_stmt3 = "SELECT COUNT(*) FROM dbo.earthquake_data WHERE mag >= 2 AND mag < 3"
    sql_stmt4 = "SELECT COUNT(*) FROM dbo.earthquake_data WHERE mag >= 3"
    
    
    cursor = conn.cursor()
    cursor.execute(sql_stmt1)
    d1 = cursor.fetchone()[0]
    cursor.execute(sql_stmt2)
    d2 = cursor.fetchone()[0]
    cursor.execute(sql_stmt3)
    d3 = cursor.fetchone()[0]
    cursor.execute(sql_stmt4)
    d4 = cursor.fetchone()[0]

    labels = ["0 to 1", "1 to 2", "2 to 3", "Above 3"]
    values = [d1, d2, d3, d4]
    
    fig = go.Figure(data=[go.Pie(labels=labels, values=values)])
    chart = fig.to_html(full_html=False)

    #return jsonify(data)
    return render_template('piechart1.html', chart=chart)
    
    #return render_template('piechart1.html', data=jsonify(response))

@app.route('/barchart1', methods =["GET", "POST"])
def barchart1():
    sql_stmt1 = "SELECT COUNT(*) FROM dbo.earthquake_data WHERE mag < 1"
    sql_stmt2 = "SELECT COUNT(*) FROM dbo.earthquake_data WHERE mag >= 1 AND mag < 2"
    sql_stmt3 = "SELECT COUNT(*) FROM dbo.earthquake_data WHERE mag >= 2 AND mag < 3"
    sql_stmt4 = "SELECT COUNT(*) FROM dbo.earthquake_data WHERE mag >= 3"
    
    
    cursor = conn.cursor()
    cursor.execute(sql_stmt1)
    d1 = cursor.fetchone()[0]
    cursor.execute(sql_stmt2)
    d2 = cursor.fetchone()[0]
    cursor.execute(sql_stmt3)
    d3 = cursor.fetchone()[0]
    cursor.execute(sql_stmt4)
    d4 = cursor.fetchone()[0]

    labels = ["0 - 1", "1 - 2", "2 - 3", "Above 3"]
    values = [d1, d2, d3, d4]
    
    fig = go.Figure(data=[go.Bar(x=labels, y=values)])
    chart = fig.to_html(full_html=False)

    #return jsonify(data)
    return render_template('barchart1.html', chart=chart)
    