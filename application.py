from flask import Flask, render_template, request, redirect, url_for, session
import pandas as pd
import numpy as np
import json 
import mysql
import requests
from itertools import islice
from contextlib import closing
import csv
import codecs
from itertools import islice
from dateutil.parser import parse
import time  #depends on the mthod used for hive metadata if not useful remove it
import pyodbc
import os
import urllib
import warnings
import re





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
        session['source location type'] = details['source location type']
        TargetType = details['Target Location Type']
        Target_Applicationid = details['Target_Applicationid']
        target_ApplicationCredential = details['target_ApplicationCredential']
        Target_Directoryid = details['Target_Directoryid']
        Target_Adlaccount = details['Target_Adlaccount']
        source_query = details['source query']
        google_drive_link=details['Public Sharable Link']
        session['onedrive link']=details['Public Downloadable Link']
        session['Delimiter of onedrive']=details['Delimiter of onedrive']
        session['delimiter']=details['Type of Delimiter']
        if session['source location type']=='Google Drive':
            session['file_id'] = google_drive_link.split('/')[-2]
        mydb = mysql.connector.connect(host="demetadata.mysql.database.azure.com",user="DEadmin@demetadata",passwd="Tredence@123",database = "deaccelator")
        cursor = mydb.cursor()
        cursor.execute(" SELECT * FROM datacatlogentry   WHERE  UserName=%s AND `FileName`=%s AND TargetType=%s   LIMIT 1 ;",(UserName,FileName,TargetType))
        account1 = cursor.fetchone()
        if account1:
            session['file exists'] = 'YES'
            session['existing file Entry ID']=account1[0]
        else:
            session['file exists'] = 'NO'
        cursor.execute("INSERT INTO datacatlogentry (UserName, DataCategory,Owner,FileName,SourceType,TargetType,Source_Query) VALUES (%s,%s, %s,%s,%s,%s,%s) ;",(UserName, DataCategory,Owner,FileName,session['source location type'],TargetType,source_query))
        mydb.commit()
        cursor.close()
        mydb = mysql.connector.connect(host="demetadata.mysql.database.azure.com",user="DEadmin@demetadata",passwd="Tredence@123",database = "deaccelator")
        cursor = mydb.cursor()
        cursor.execute(" SELECT EntryID FROM datacatlogentry  WHERE UserName=%s AND `Owner`=%s ORDER BY EntryID DESC LIMIT 1 ;",(UserName,Owner))
        data=cursor.fetchone()
        df = pd.DataFrame(data)
        session['EntryID']=int(df.iat[0,0])
        if session['source location type'] == 'MySql':
            cursor.execute(" INSERT INTO parameter (EntryId, Source_Type, Source_Parameter_1, Source_Parameter_2, Source_Parameter_3, Source_Parameter_4, Target_Type, Target_Parameter_1, target_AParameter_2, Target_Parameter_3, Target_Parameter_4) VALUES(%s,%s, %s,%s,%s, %s,%s, %s,%s,%s, %s) ;",(session['EntryID'],session['source location type'],session['hostname'],session['user'],session['password'],session['database name' ],TargetType,Target_Applicationid,target_ApplicationCredential,Target_Directoryid,Target_Adlaccount))
        elif session['source location type'] == 'Google Drive':
            cursor.execute(" INSERT INTO parameter (EntryId, Source_Type, Source_Parameter_1, Source_Parameter_2, Target_Type, Target_Parameter_1, target_AParameter_2, Target_Parameter_3, Target_Parameter_4) VALUES(%s,%s,%s, %s,%s, %s,%s,%s, %s) ;",(session['EntryID'],session['source location type'],session['file_id'],session['delimiter'],TargetType,Target_Applicationid,target_ApplicationCredential,Target_Directoryid,Target_Adlaccount))
        elif session['source location type'] == 'One Drive':
            cursor.execute(" INSERT INTO parameter (EntryId, Source_Type, Source_Parameter_1, Source_Parameter_2, Target_Type, Target_Parameter_1, target_AParameter_2, Target_Parameter_3, Target_Parameter_4) VALUES(%s,%s,%s, %s,%s, %s,%s,%s, %s) ;",(session['EntryID'],session['source location type'],session['onedrive link'],session['Delimiter of onedrive'],TargetType,Target_Applicationid,target_ApplicationCredential,Target_Directoryid,Target_Adlaccount))
        elif session['source location type'] == 'Hive':
            cursor.execute(" INSERT INTO parameter (EntryId, Source_Type, Target_Type, Target_Parameter_1, target_AParameter_2, Target_Parameter_3, Target_Parameter_4) VALUES(%s,%s,%s, %s,%s,%s, %s) ;",(session['EntryID'],session['source location type'],TargetType,Target_Applicationid,target_ApplicationCredential,Target_Directoryid,Target_Adlaccount))
        mydb.commit()
        cursor.close()
        if session['source location type'] == 'Hive':
            return redirect(url_for('hive_metadata_1'))
        else:
            return redirect(url_for('index'))
    return render_template('overview.html')

@app.route("/hive_metadata_1", methods=['GET', 'POST'])    #series for hive metadata capture
def hive_metadata_1():
    SERVER_NAME="testsrc.azurehdinsight.net"   #https://testsrc.azurehdinsight.net/
    DATABASE_NAME="default"
    USERID="admin"  #admin
    PASSWORD="Tredence@123"  #Tredence@123
    DB_DRIVER="Microsoft Hive ODBC Driver"  
    driver = 'DRIVER={' + DB_DRIVER + '}'
    server = 'Host=' + SERVER_NAME + ';Port=443;UseNativeQuery=1'
    database = 'Schema=' + DATABASE_NAME
    hiveserv = 'HiveServerType=2'
    auth = 'AuthMech=6'
    uid = 'UID=' + USERID
    pwd = 'PWD=' + PASSWORD
    CONNECTION_STRING = ';'.join([driver,server,database,hiveserv,auth,uid,pwd])
    #print(CONNECTION_STRING)
    connection = pyodbc.connect(CONNECTION_STRING, autocommit=True)
    cursor=connection.cursor()
    queryString34 = """ desc testdb.actor  ; """
    a = pd.read_sql(queryString34,connection)
    df = pd.DataFrame()
    df['ColumnName']=a['col_name']
    df['DataType']=a['data_type']
    df3 = df.assign(ColumnNumber=[i+1 for i in range(len(df))])[['ColumnNumber'] + df.columns.tolist()]
    df3['Nullable']=''
    df3['Default']=''
    df3['Description']=''
    value = session['file exists']
    return render_template("metadataV3.html", column_names=df3.columns.values, row_data=list(df3.values.tolist()), zip=zip, value=value)

# @app.route('/pythonlogin/metadata') # is not useful for now
# def metadata():
#     # Check if user is loggedin
#     if 'loggedin' in session:
#         # User is loggedin show them the home page
#         return render_template('metadataV2.html', username=session['username'])
#     # User is not loggedin redirect to login page
#     return redirect(url_for('login'))

def conv2(s):      # used for determing datatypes of flat file
    try:
        val = int(s)
        return val
    except ValueError:
        try:
            val = float(s)
            return val
        except ValueError:
            try:
                s=parse(s)
            except ValueError:
                pass    
    return s

def get_confirm_token(response):
            for key, value in response.cookies.items():
                if key.startswith('download_warning'):
                    return value
            return None


@app.route('/metadata', methods=['GET', 'POST'])
def index():                                     # for flat file and mysql metadata
    SourceType = session['source location type']
    if SourceType == 'MySql':
        mydb = mysql.connector.connect(host=session['hostname'],user=session['user'],passwd=session['password'],database = session['database name'])
        cursor = mydb.cursor()
        cursor.execute("DROP VIEW IF EXISTS temp")
        cursor.execute("CREATE VIEW temp AS "+session['source query']+" LIMIT 1 ")
        cursor.execute("DESCRIBE temp")
        data = cursor.fetchall() 
        df = pd.DataFrame(data, columns='ColumnName DataType Nullable PrimaryKey Default Description'.split())
        df = df.assign(ColumnNumber=[i+1 for i in range(len(df))])[['ColumnNumber'] + df.columns.tolist()]
        df1=  df.drop(['PrimaryKey'], axis = 1)
        cursor.execute(" DROP VIEW temp ")
        mydb.commit()
        cursor.close()
        value = session['file exists']
        return render_template("metadataV3.html", column_names=df1.columns.values, row_data=list(df1.values.tolist()), zip=zip, value=value)
    elif SourceType == 'Google Drive':
        URL = 'https://docs.google.com/uc?export=download'
        session1 = requests.Session()
        file_id = session['file_id']
        response = session1.get(URL, params = { 'id' : file_id }, stream = True)
        token = get_confirm_token(response)
        if token:
            params = { 'id' : file_id, 'confirm' : token }
            response = session1.get(URL, params = params, stream = True)
        a = session['delimiter']
        with closing(response) as r:
            reader = csv.reader(codecs.iterdecode(r.iter_lines(), 'utf-8'), delimiter=a , quotechar='"',quoting=csv.QUOTE_MINIMAL )
            lst = []
            a=[]
            for row in islice(reader,0,10):
                for cell in row:
                    y=conv2(cell)
                    a.append(y)
                lst.append(a)
                a=[]
        df = pd.DataFrame(lst[1:],columns=lst[0])
        df2=pd.DataFrame(df.dtypes,index=None,columns='data_type'.split())
        df3 = pd.DataFrame(df.columns,columns='ColumnName'.split())
        df4=df2.replace(['int64','float64','datetime64[ns]','object'],['int','float','datetime','string'])
        df4.index = df3.index
        df3['DataType']=df4['data_type']
        df3 = df3.assign(ColumnNumber=[i+1 for i in range(len(df3))])[['ColumnNumber'] + df3.columns.tolist()]
        df3['Nullable']=''
        df3['Default']=''
        df3['Description']=''
        value = session['file exists']
        return render_template("metadataV3.html", column_names=df3.columns.values, row_data=list(df3.values.tolist()), zip=zip, value=value)
    elif SourceType == 'One Drive':           #session['onedrive link'],session['Delimiter of onedrive'],
        session1 = requests.Session()
        dwn_url = session['onedrive link']
        response = session1.get(dwn_url, stream = True)
        token = get_confirm_token(response)
        if token:
            params = { 'confirm' : token }
            response = session1.get(dwn_url, params = params, stream = True)
        a = session['Delimiter of onedrive']
        with closing(response) as r:
            reader = csv.reader(codecs.iterdecode(r.iter_lines(), 'utf-8'), delimiter=a , quotechar='"',quoting=csv.QUOTE_MINIMAL )
            lst = []
            a=[]
            for row in islice(reader,0,10):
                for cell in row:
                    y=conv2(cell)
                    a.append(y)
                lst.append(a)
                a=[]
        df = pd.DataFrame(lst[1:],columns=lst[0])
        df2=pd.DataFrame(df.dtypes,index=None,columns='data_type'.split())
        df3 = pd.DataFrame(df.columns,columns='ColumnName'.split())
        df4=df2.replace(['int64','float64','datetime64[ns]','object'],['int','float','datetime','string'])
        df4.index = df3.index
        df3['DataType']=df4['data_type']
        df3 = df3.assign(ColumnNumber=[i+1 for i in range(len(df3))])[['ColumnNumber'] + df3.columns.tolist()]
        df3['Nullable']=''
        df3['Default']=''
        df3['Description']=''
        value = session['file exists']
        return render_template("metadataV3.html", column_names=df3.columns.values, row_data=list(df3.values.tolist()), zip=zip, value=value)


@app.route('/Rollbackmetadata', methods=['GET', 'POST'])
def Rollbackmetadata():
    mydb = mysql.connector.connect(host="demetadata.mysql.database.azure.com",user="DEadmin@demetadata",passwd="Tredence@123",database = "deaccelator")
    cursor = mydb.cursor()
    cursor.execute("DELETE FROM metadata WHERE EntryID = %s ;"%(session['EntryID']))
    mydb.commit()
    cursor.close()
    if session['source location type'] == 'Hive':
        return redirect(url_for('hive_metadata_1'))
    else:
        return redirect(url_for('index'))    


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
        session['json_metadata']=str(df1.to_json)
        print(session['json_metadata'])
        cols = "`,`".join([str(i) for i in df1.columns.tolist()])
        for i,row in df1.iterrows():
            sql = "INSERT INTO `metadata` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
            cursor.execute(sql, tuple(row))
        df5 = df1['DataType']
        mydb.commit()
        cursor.close()
        if session['file exists']=='YES' :
            mydb1 = mysql.connector.connect(host="demetadata.mysql.database.azure.com",user="DEadmin@demetadata",passwd="Tredence@123",database = "deaccelator")
            cursor1 = mydb1.cursor()
            cursor1.execute(' SELECT DataType FROM metadata Where EntryID = %s ;'%(session['existing file Entry ID']))
            data1 = cursor1.fetchall()
            df6 = pd.DataFrame(data1, columns='DataType'.split())
            df7 = df6['DataType']
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



import requests,json

@app.route('/pythonlogin/metadata4', methods=['GET', 'POST'])
def index2():
    headers = {'Authorization': 'Bearer dapi042eca35a8dd2f707b2562849e33f013'}
    data = '{ "job_id" : 3 , "notebook_params": { "entryid": ' +str(session['EntryID'])+ ' } }'
    response = requests.post('https://adb-6971132450799346.6.azuredatabricks.net/api/2.0/jobs/run-now', headers=headers, data=data)
    print(response)
    print(session['json_metadata'])
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
    print(response)
    return redirect(url_for('home')) 

@app.route('/pythonlogin/metadata6', methods=['GET', 'POST'])
def replace():
    mydb = mysql.connector.connect(host="demetadata.mysql.database.azure.com",user="DEadmin@demetadata",passwd="Tredence@123",database = "deaccelator")
    cursor = mydb.cursor()
    cursor.execute(' UPDATE datacatlogentry Set Operation = %s Where EntryID = %s ; ',('Insert',int(session['EntryID'])))
    mydb.commit()
    cursor.close()
    headers = {'Authorization': 'Bearer dapi042eca35a8dd2f707b2562849e33f013'}
    data = '{ "job_id" : 3 , "notebook_params": { "entryid": ' +str(session['EntryID'])+ ' } }'
    response = requests.post('https://adb-6971132450799346.6.azuredatabricks.net/api/2.0/jobs/run-now', headers=headers, data=data)
    print(response)
    return redirect(url_for('home')) 




if __name__=="__main__":
    app.run(debug=True)

