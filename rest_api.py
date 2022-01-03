from flask import Flask, request, jsonify, redirect
from flask_mysqldb import MySQL
import json, os, uuid, time, atexit, requests
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, ContentSettings, __version__
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)
  
app.config['MYSQL_HOST'] = 'finteachdb.mysql.database.azure.com'
app.config['MYSQL_USER'] = 'finteach_admin@finteachdb'
app.config['MYSQL_PASSWORD'] = ''
app.config['MYSQL_DB'] = 'finteach'
 
mysql = MySQL(app)

# APScheduler library that will keep calling the stored procedures
def call_stored_procedures():
    result = requests.get('https://finteachrestapi.azurewebsites.net/stored_procedures')

scheduler = BackgroundScheduler()
scheduler.add_job(func=call_stored_procedures, trigger="interval", seconds=10)
scheduler.start()

# Shut down the scheduler when exiting the app
atexit.register(lambda: scheduler.shutdown())

@app.route('/stored_procedures', methods=['GET'])
def stored_procedures():
    try:
        sqlstatement = 'CALL update_tables()'
        
        cur = mysql.connection.cursor()
        cur.execute(sqlstatement)
        cur.connection.commit()
    
        return ""

    except Exception as e:
        return "ERROR: " + str(e)

@app.route('/uploadfile', methods=['GET', 'POST'])
def uploadfile():
    try:
        # Create the BlobServiceClient object which will be used to create a container client
        connect_str = "DefaultEndpointsProtocol=https;AccountName=finteachstorage;AccountKey=qukKdq1dyxKmxjenEbV0JTjB82WGSQYwqRn8mfVKHGtPuNivZ76suwVzER8LISRnkAW7siwGycUfZH4svylxEw==;EndpointSuffix=core.windows.net"
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        # Get the file the user uploaded and append the unique file id. Also get redirect url
        uploaded_file = request.files['file']
        file_id = request.form['file_id']
        file_name = file_id + uploaded_file.filename
        redirect_url = request.form['redirect_url']

        # Change the content-type based on the file-type
        # https://stackoverflow.com/questions/23714383/what-are-all-the-possible-values-for-http-content-type-header
        file_type = uploaded_file.content_type
        my_content_settings = ContentSettings(content_type=file_type)

        # Create a blob client using the finteachblob container and the name of the file
        blob_client = blob_service_client.get_blob_client(container="finteachblob", blob=file_name)
        
        # Upload the created file
        blob_client.upload_blob(uploaded_file, content_settings=my_content_settings)

        return redirect(redirect_url)
    except Exception as e:
        return "ERROR: " + str(e)

@app.route('/select', methods=['GET'])
def select():
    try:
        sqlstatement = request.args['query']
        
        cur = mysql.connection.cursor()
        cur.execute(sqlstatement)

        row_headers=[x[0] for x in cur.description] #this will extract row headers
        rv = cur.fetchall()
        json_data=[]
        for result in rv:
            json_data.append(dict(zip(row_headers,result)))
        cur.close()

        return jsonify(json_data)

    except Exception as e:
        return "ERROR: " + str(e)

@app.route('/insert', methods=['POST'])
def insert():
    try:
        sqlstatement = request.form['sql_statement']
        redirect_url = request.form['redirect_url']
        
        cur = mysql.connection.cursor()
        cur.execute(sqlstatement)
        cur.connection.commit()
    
        return redirect(redirect_url)

    except Exception as e:
        return "ERROR: " + str(e)

@app.route('/triggers', methods=['GET', 'POST'])
def triggers():
    try: 
        sqlstatement = request.args['query']
        
        cur = mysql.connection.cursor()
        cur.execute(sqlstatement)
        cur.connection.commit()
           
        return ""
    except Exception as e:
        return "ERROR: " + str(e)  
    
if __name__ == '__main__':
    app.run()