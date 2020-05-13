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
import json 
import mysql



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

@app.route('/pythonlogin/overview', methods=['GET', 'POST'])
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
        mydb = mysql.connector.connect(host="demetadata.mysql.database.azure.com",user="DEadmin@demetadata",passwd="Tredence@123",database = "deaccelator")
        cursor = mydb.cursor()
        cursor.execute(" SELECT * FROM datacatlogentry   WHERE  UserName=%s AND `FileName`=%s AND TargetType=%s   LIMIT 1 ;",(UserName,FileName,TargetType))
        account1 = cursor.fetchone()
        if account1:
            session['file exists'] = 'YES'
            session['existing file Entry ID']=account1[0]
            print(session['existing file Entry ID'])
        else:
            session['file exists'] = 'NO'
        print(session['file exists'])
        cursor.execute("INSERT INTO datacatlogentry (UserName, DataCategory,Owner,FileName,SourceType,TargetType,Source_Query) VALUES (%s,%s, %s,%s,%s,%s,%s) ;",(UserName, DataCategory,Owner,FileName,SourceType,TargetType,source_query))
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

@app.route('/pythonlogin/metadata') # is not useful for now
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






@app.route('/metadata', methods=['GET', 'POST'])
def index():
    mydb = mysql.connector.connect(host=session['hostname'],user=session['user'],passwd=session['password'],database = session['database name'])
    cursor = mydb.cursor()
    cursor.execute("DROP VIEW IF EXISTS temp")
    cursor.execute("CREATE VIEW temp AS "+session['source query']+" LIMIT 1 ")
    cursor.execute("DESCRIBE temp")
    data = cursor.fetchall() 
    df = pd.DataFrame(data, columns='ColumnName DataType Nullable PrimaryKey Default Description'.split())
    df = df.assign(ColumnNumber=[i+1 for i in range(len(df))])[['ColumnNumber'] + df.columns.tolist()]
    cursor.execute(" DROP VIEW temp ")
    mydb.commit()
    cursor.close()
    value = session['file exists']
    return render_template("metadataV3.html", column_names=df.columns.values, row_data=list(df.values.tolist()), zip=zip, value=value)


@app.route('/ingest', methods=['GET', 'POST'])
def index1():
    if request.method == "POST":
        newform = request.form.getlist
        # EntryID = newform('EntryID')
        ColumnNumber = newform('ColumnNumber')
        ColumnName = newform('ColumnName')	
        DataType = newform('DataType')
        Nullable = newform('Nullable')
        PrimaryKey = newform('PrimaryKey')
        Default = newform('Default')
        Column_description = newform('Description')
        mydb = mysql.connector.connect(host="demetadata.mysql.database.azure.com",user="DEadmin@demetadata",passwd="Tredence@123",database = "deaccelator")
        cursor = mydb.cursor()
        df4 = pd.DataFrame(list(zip(ColumnNumber,ColumnName,DataType,Nullable,PrimaryKey,Default,Column_description)), columns =['ColumnNumber','ColumnName','DataType','Nullable','PrimaryKey','Default','Description'])
        df1 = df4.assign(EntryID=session['EntryID'])[['EntryID'] + df4.columns.tolist()]
        cols = "`,`".join([str(i) for i in df1.columns.tolist()])
        for i,row in df1.iterrows():
            sql = "INSERT INTO `metadata` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
            cursor.execute(sql, tuple(row))
        print(df1)
        df5 = df1['DataType']
        print(df5)
        mydb.commit()
        cursor.close()
        if session['file exists']=='YES' :
            mydb1 = mysql.connector.connect(host="demetadata.mysql.database.azure.com",user="DEadmin@demetadata",passwd="Tredence@123",database = "deaccelator")
            cursor1 = mydb1.cursor()
            cursor1.execute(' SELECT DataType FROM metadata Where EntryID = %s ;'%(session['existing file Entry ID']))
            data1 = cursor1.fetchall()
            df6 = pd.DataFrame(data1, columns='DataType'.split())
            df7 = df6['DataType']
            print(df7)
            print(df5.equals(df7))
            mydb.commit()
            cursor.close()
            if df5.equals(df7) :
                value = session['file exists']
                return render_template("metadataV4.html", column_names=df4.columns.values, row_data=list(df4.values.tolist()), zip=zip, value=value)
            else :
                value = 'YES, But Metadata is not matching'
                return render_template("metadataV4.html", column_names=df4.columns.values, row_data=list(df4.values.tolist()), zip=zip, value=value)
            
        else:
            value = session['file exists']
            return render_template("metadataV4.html", column_names=df4.columns.values, row_data=list(df4.values.tolist()), zip=zip, value=value)
    return render_template("metadataV3.html")

# @app.route('/pythonlogin/metadata5', methods=['GET', 'POST'])
# def popup_test():
#     if (session['file exists'] == 'YES') :
#         print('file exists') # wrte code to invoke script
#     else:
#         return redirect(url_for('index2')) 


import requests,json

@app.route('/pythonlogin/metadata4', methods=['GET', 'POST'])
def index2():
    headers = {'Authorization': 'Bearer dapi042eca35a8dd2f707b2562849e33f013'}
    data = '{ "job_id" : 3 , "notebook_params": { "entryid": ' +str(session['EntryID'])+ ' } }'
    response = requests.post('https://adb-6971132450799346.6.azuredatabricks.net/api/2.0/jobs/run-now', headers=headers, data=data)
    return redirect(url_for('home')) 

@app.route('/pythonlogin/metadata5', methods=['GET', 'POST'])
def append():
    mydb = mysql.connector.connect(host="demetadata.mysql.database.azure.com",user="DEadmin@demetadata",passwd="Tredence@123",database = "deaccelator")
    cursor = mydb.cursor()
    cursor.execute(" UPDATE datacatlogentry Set Operation = %s Where entryid = %s ;",('Upsert',int(session['EntryID'])))
    mydb.commit()
    cursor.close()
    headers = {'Authorization': 'Bearer dapi042eca35a8dd2f707b2562849e33f013'}
    data = '{ "job_id" : 3 , "notebook_params": { "entryid": ' +str(session['EntryID'])+ ' } }'
    response = requests.post('https://adb-6971132450799346.6.azuredatabricks.net/api/2.0/jobs/run-now', headers=headers, data=data)
    return redirect(url_for('home')) 

@app.route('/pythonlogin/metadata6', methods=['GET', 'POST'])
def replace():
    mydb = mysql.connector.connect(host="demetadata.mysql.database.azure.com",user="DEadmin@demetadata",passwd="Tredence@123",database = "deaccelator")
    cursor = mydb.cursor()
    cursor.execute(' UPDATE datacatlogentry Set Operation = %s Where EntryID = %s ; ',('Overwrite',int(session['EntryID'])))
    mydb.commit()
    cursor.close()
    headers = {'Authorization': 'Bearer dapi042eca35a8dd2f707b2562849e33f013'}
    data = '{ "job_id" : 3 , "notebook_params": { "entryid": ' +str(session['EntryID'])+ ' } }'
    response = requests.post('https://adb-6971132450799346.6.azuredatabricks.net/api/2.0/jobs/run-now', headers=headers, data=data)
    return redirect(url_for('home')) 




if __name__=="__main__":
    app.run(debug=True)

