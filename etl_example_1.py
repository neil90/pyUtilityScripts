import pandas as pd
import pyodbc
import json
import smtplib
from urllib.request import urlopen
import sqlalchemy as sa
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import traceback
 
def get_data():
   
    cnxn = pyodbc.connect("DSN=HIVE_ODBC",autocommit=True)
   
    sql = """
    SELECT empid, wrkstate, emp_title, zip as personal_zip
    FROM company.employee_table
    WHERE wrkstate='CA' LIMIT 10
    """
   
    data = pd.read_sql(sql,cnxn)
   
    data['work_zip'] = 92612
   
    return data
   
def transform_data(dataframe):
    data = dataframe
 
   def calc_dist(api_key,wrkzip,reszip):
 
        response = urlopen("https://www.zipcodeapi.com/rest/%s/distance.json"
                            "/%s/%s/mile.json" % (api_key,wrkzip,reszip))
 
        content = response.read()
        data = json.loads(content.decode('utf-8'))
        return data['distance']
   
    zapi = json.load(open(r'json_yaml_files\zip_api.json'))
   
    data['distance_to_work'] = data.apply(lambda x: calc_dist(
                                zapi['api_key'], x['work_zip'],
                                x['personal_zip']), axis=1)
   
    return data
 
def load_data(dataframe):
    data = dataframe
    engine = sa.create_engine('mssql+pyodbc://@sqlTestServer')
   
    data.to_sql('dist_table', engine, if_exists='append', index = False)
   
def email(info):
    fromaddr = "neilpatel90@gmail.com"
    toaddr = "neilpatel90@gmail.com"
   
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = "Worker Distance Job Failed!"
   
    body = "See below for the stacktrace:\n\n{0}".format(info)
    msg.attach(MIMEText(body, 'plain'))
 
    server = smtplib.SMTP('smtp.gmail.com', 587)
    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    server.quit()
   
 
try:
    df = get_data()
    df = transform_data(df)
    load_data(df)
except:
    tb = str(traceback.format_exc())
    email(tb)
