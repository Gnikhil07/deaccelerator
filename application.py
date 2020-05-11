from flask import Flask, render_template, request, redirect, url_for, session
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
from flask_mysqldb import MySQL
import MySQLdb.cursors
import re
from datetime import datetime
import pandas as pd
import numpy as np
import pymysql
import mysql

import requests,json


app = Flask(__name__)

# Change this to your secret key (can be anything, it's for extra protection)
app.secret_key = 'your secret key'

# Enter your database connection details below
import mysql.connector





@app.route("/", methods=['GET', 'POST'])
def login():
    # Output message if something goes wrong...
    msg = ''
    # Check if "username" and "password" POST requests exist (user submitted form)
    if request.method == 'POST' and 'username' in request.form and 'password' in request.form:
        # Create variables for easy access
        username = request.form['username']
        password = request.form['password']
        mydb = mysql.connector.connect(host="demetadata.mysql.database.azure.com",user="DEadmin@demetadata",passwd="Tredence@123",database = "deaccelator")
        cursor = mydb.cursor()
        cursor.execute('SELECT * FROM userdetails WHERE UserName = %s AND UserPassword = %s ;', (username, password))
        # Fetch one record and return result
        account = cursor.fetchone()
        # If account exists in accounts table in out database
        if account:
            # Create session data, we can access this data in other routes
            session['loggedin'] = True
            # session['Name'] = account['Name']
            session['username'] = account[0]
            # Redirect to home page
            return redirect(url_for('home'))
        else:
            # Account doesnt exist or username/password incorrect
            msg = 'Incorrect username/password!'
    return render_template('index.html', msg=msg)


@app.route('/logout')
def logout():
    # Remove session data, this will log the user out
   session.pop('loggedin', None)
   session.pop('id', None)
   session.pop('username', None)
   # Redirect to login page
   return redirect(url_for('login'))

@app.route('/home')
def home():
    # Check if user is loggedin
    if 'loggedin' in session:
        # User is loggedin show them the home page
        return render_template('home.html', username=session['username'])
    # User is not loggedin redirect to login page
    return redirect(url_for('login'))

@app.route('/overview')
def overview():
    # Check if user is loggedin
    if 'loggedin' in session:
        # User is loggedin show them the home page
        return render_template('overview.html', username=session['username'])
    # User is not loggedin redirect to login page
    return redirect(url_for('login'))

@app.route('/overview', methods=['GET', 'POST'])
def overviewform():
    if request.method == "POST":
        details = request.form
        session['hostname']=details['hostname']
        session['user']=details['User']
        session['password']=details['password']
        session['database name' ]= details['database name']
        session['source query']=details['source query']
        session['Target Dataset Name']=details['Target Dataset Name']
        UserName = session['username']
        DataCategory = details['Dataset Catergory']
        Owner = details['Data Owner']
        FileName = details['Target Dataset Name']
        SourceType = details['source location type']
        TargetType = details['Target Location Type']
        Target_Applicationid = details['Target_Applicationid']
        target_ApplicationCredential = details['target_ApplicationCredential']
        Target_Directoryid = details['Target_Directoryid']
        Target_Adlaccount = details['Target_Adlaccount']
        source_query = details['source query']
        operation_type = details['operation type']
        mydb = mysql.connector.connect(host="demetadata.mysql.database.azure.com",user="DEadmin@demetadata",passwd="Tredence@123",database = "deaccelator")
        cursor = mydb.cursor()
        cursor.execute("INSERT INTO datacatlogentry (UserName, DataCategory,Owner,FileName,SourceType,TargetType,Operation,Source_Query) VALUES (%s,%s, %s,%s,%s, %s,%s,%s) ;",(UserName, DataCategory,Owner,FileName,SourceType,TargetType,operation_type,source_query))
        mydb.commit()
        cursor.close()
        mydb = mysql.connector.connect(host="demetadata.mysql.database.azure.com",user="DEadmin@demetadata",passwd="Tredence@123",database = "deaccelator")
        cursor = mydb.cursor()
        cursor.execute(" SELECT EntryID FROM datacatlogentry   ORDER BY EntryID DESC LIMIT 1 ;")
        data=cursor.fetchone()
        df = pd.DataFrame(data)
        session['EntryID']=int(df.iat[0,0])
        cursor.execute(" INSERT INTO parameter (EntryId, Source_Type, Source_HostName, Source_UserName, Source_Password, Source_Database, Target_Type, Target_Applicationid, target_ApplicationCredential, Target_Directoryid, Target_Adlaccount) VALUES(%s,%s, %s,%s,%s, %s,%s, %s,%s,%s, %s) ;",(session['EntryID'],SourceType,session['hostname'],session['user'],session['password'],session['database name' ],TargetType,Target_Applicationid,target_ApplicationCredential,Target_Directoryid,Target_Adlaccount))
        mydb.commit()
        cursor.close()

        return redirect(url_for('index'))
    return render_template('overview.html')

@app.route('/metadata') # is not useful for now
def metadata():
    # Check if user is loggedin
    if 'loggedin' in session:
        # User is loggedin show them the home page
        return render_template('metadataV2.html', username=session['username'])
    # User is not loggedin redirect to login page
    return redirect(url_for('login'))

# @app.route('/pythonlogin/metadata')
# def metadata():

#     if request.method == "POST":
#         details = request.form
#         # Source_Location = details['Source Location']
#         # df1 = pd.read_csv(rSource_Location)
#         df2=pd.read_csv(r'C:\Users\revanth.tirumala\Downloads\corona-virus-report\covid_19_clean_complete.csv')
#         return df2.to_html()
#     return render_template('metadataV2.html', username=session['username'])
#     # return redirect(url_for('login'))






@app.route('/metadata2', methods=['GET', 'POST'])
def index():
    mydb = mysql.connector.connect(host=session['hostname'],user=session['user'],passwd=session['password'],database = session['database name'])
    cursor = mydb.cursor()
    cursor.execute("DROP VIEW IF EXISTS temp")
    cursor.execute("CREATE VIEW temp AS "+session['source query']+" LIMIT 1 ")
    cursor.execute("DESCRIBE temp")
    data = cursor.fetchall() 
    df = pd.DataFrame(data, columns='ColumnName DataType Nullable PrimaryKey Default Description'.split())
    df = df.assign(ColumnNumber=[i+1 for i in range(len(df))])[['ColumnNumber'] + df.columns.tolist()]
    df1 = df.assign(EntryID=session['EntryID'])[['EntryID'] + df.columns.tolist()]
    cursor.execute(" DROP VIEW temp ")
    mydb.commit()
    cursor.close()
    return render_template("metadataV3.html", column_names=df1.columns.values, row_data=list(df1.values.tolist()), zip=zip)


# def listoftuples(a):
#     i=0
#     j=8
#     listlen = len(a)
#     reslist=[]
#     while ((i+8<=listlen) and (j<=listlen)):
#         splitlist = a[i:j]
#         reslist.append(tuple(splitlist))
#         i = i+8
#         j = j+8
#     return reslist


@app.route('/metadata3', methods=['GET', 'POST'])
def index1():
    if request.method == "POST":
        # a= list(request.form.getlist('imp'))
        # c = listoftuples(a)
        # cur = mysql.connection.cursor()
        # for item in c:
        #     cur.execute("INSERT INTO metadata VALUES(%s, %s, %s, %s, %s, %s, %s, %s ) ", item )
        # cur.execute(" SELECT * FROM metadata ")
        # mysql.connection.commit()
        # cur.close()
        # change id and name in table in metadatav3 to {{col}} to use the below method
        newform = request.form.getlist
        EntryID = newform('EntryID')
        ColumnNumber = newform('ColumnNumber')
        ColumnName = newform('ColumnName')	
        DataType = newform('DataType')
        Nullable = newform('Nullable')
        PrimaryKey = newform('PrimaryKey')
        Default = newform('Default')
        Column_description = newform('Description')
        mydb = mysql.connector.connect(host="demetadata.mysql.database.azure.com",user="DEadmin@demetadata",passwd="Tredence@123",database = "deaccelator")
        cursor = mydb.cursor()
        df4 = pd.DataFrame(list(zip(EntryID, ColumnNumber,ColumnName,DataType,Nullable,PrimaryKey,Default,Column_description)), columns =['EntryID', 'ColumnNumber','ColumnName','DataType','Nullable','PrimaryKey','Default','Description'])
        cols = "`,`".join([str(i) for i in df4.columns.tolist()])
        for i,row in df4.iterrows():
            sql = "INSERT INTO `metadata` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
            cursor.execute(sql, tuple(row))
        mydb.commit()
        cursor.close()
        return redirect(url_for('index2'))
    return render_template("metadataV3.html")



@app.route('/metadata4', methods=['GET', 'POST'])
def index2():
   headers = {'Authorization': 'Bearer dapi042eca35a8dd2f707b2562849e33f013'}
   data = '{ "job_id" : 3 , "notebook_params": { "entryid": ' +str(session['EntryID'])+ ' } }'
   response = requests.post('https://adb-6971132450799346.6.azuredatabricks.net/api/2.0/jobs/run-now', headers=headers, data=data)
   return redirect(url_for('home')) 


if __name__=="__main__":
    app.run(debug=True)

