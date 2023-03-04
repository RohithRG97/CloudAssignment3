from flask import Flask, render_template, request, url_for
import pypyodbc as odbc

app = Flask(__name__)

server = 'cloudsqlserver01.database.windows.net'
database = 'DemoSQLServerDB'
connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:freecloudsqlserver001.database.windows.net,1433;Database=DemoSQLServerDB;Uid=RohithGurram;Pwd={freecloudsqlserver@123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
conn = odbc.connect(connection_string)


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


if __name__ == "__main__":
    app.run(debug=True)