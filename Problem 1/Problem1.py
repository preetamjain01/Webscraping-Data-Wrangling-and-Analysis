# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import urllib.request
from bs4 import BeautifulSoup
import csv
import logging
import os
import zipfile
import boto.s3
import sys
from boto.s3.key import Key
import time
import datetime
from configparser import ConfigParser
from urllib.request import urlopen

log_file = logging.getLogger()
log_file.setLevel(logging.DEBUG)

log_copy = logging.FileHandler('log_file_1.log')
log_copy.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_copy.setFormatter(formatter)
log_file.addHandler(log_copy)

log_console = logging.StreamHandler(sys.stdout)
log_console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s - %(message)s')
log_console.setFormatter(formatter)
log_file.addHandler(log_console)

argLen = len(sys.argv)

config = ConfigParser()

config_file = os.path.join(os.path.dirname(__file__), '/data/config.ini')

config.read(config_file)
default = config['aws.data']
accessKey = default['accessKey']
secretAccessKey = default['secretAccessKey']
inputLocation = default['inputLocation']
cik = default['cik']
accNum = default['accessionNumber']

if inputLocation not in ['APNortheast', 'APSoutheast', 'APSoutheast2', 'EU', 'EUCentral1', 'SAEast', 'USWest',
                         'USWest2']:
    inputLocation = 'Default'

logging.info("Access Key = %s" % accessKey)
logging.info("Secret Access Key = %s" % secretAccessKey)
logging.info("Location = %s" % inputLocation)
logging.info("CIK = %s" % cik)
logging.info("accession number = %s" % accNum)

print("CIK=", cik)
print("Accession Number=", accNum)
print("Access Key=", accessKey)
print("Secret Access Key=", secretAccessKey)
print("Location=", inputLocation)


############### Validating CIK and Accession Number ###############

if not cik or not accessionNumber:
    logging.warning(
        'CIK or AccessionNumber was not mentioned, assuming the values to be 51143 and 0000051143-13-000007 respectively. This is original data of Walmart')
    cik = '51143'
    accessionNumber = '0000051143-13-000007'
else:
    logging.info('CIK: %s and AccessionNumber: %s given'%( cik, accessionNumber))

############### Validate amazon keys ###############
if not accessKey or not secretAccessKey:
    logging.warning('Access Key and Secret Access Key not provided!!')
    print('Access Key and Secret Access Key not provided!!')
    exit()

AWS_ACCESS_KEY_ID = accessKey
AWS_SECRET_ACCESS_KEY = secretAccessKey

try:
    conn = boto.connect_s3(AWS_ACCESS_KEY_ID,
                           AWS_SECRET_ACCESS_KEY)

    print("Connected to S3")

except:
    logging.info("Amazon keys are invalid!!")
    print("Amazon keys are invalid!!")
    exit()

############### Create the URL by inputed CIK and ACC_No ###############

url_start= "https://www.sec.gov/Archives/edgar/data/"

if not cik or not accNum:
    print('CIK and Accession number not given. Exiting the program')
else:
    print('CIK - %s' % (cik))
    print('Accession Number - %s' %( accNum))

url_final= url_start+cik.lstrip('0')+"/"+ accNum.replace('-','')+"/"+accNum+"-index.html"

print('Final url is: %s'%(url_final))
logging.info("URL generated is: " + url_final)

#connect to a URL
website = urlopen(url_final)

#read html code
html = website.read()
soup=BeautifulSoup(html,"lxml")

#use soup to get all the links
url_10q=""

try:
    for link in soup.findAll('a'):
        print (link.get('href'))
        url_10qE= link.get('href')
        if url_10qE.endswith('10q.htm'):
            url_10q=url_10qE
    
    if url_10q is "":
        logging.info("Invalid URL!!!")
        print("Invalid URL!!!")
        exit()

except urllib.error.HTTPError as err:
    logging.warning("Invalid CIK or AccNo")
    exit()
        

print('10q url is: %s' %(url_10q))
        
       
url_10q= "https://www.sec.gov"+url_10q

print('Complete 10q url is: %s' %(url_10q))
logging.info('Complete 10q url is: %s' %(url_10q))

if not os.path.exists('Extracted_Data_csv'):
    os.makedirs('Extracted_Data_csv')


############### Extracting 10q filings in python table ###############


page = urllib.request.urlopen(url_10q)
soup = BeautifulSoup(page, "lxml")
    
all_tables=soup.select("table")

my_tables=[]
for table in all_tables:
        my_tables.append([[td.text.replace("\n", " ").replace("\xa0"," ") for td in row.find_all("td")] for row in table.select("tr + tr")])

logging.info('Tables successfully extracted to csv')    

#print(len(my_tables))
#saving valid tables in CSV files
for tab in my_tables:
    if my_tables.index(tab) >=9 and my_tables.index(tab)<=109:
        with open(os.path.join('Extracted_csvs', str(my_tables.index(tab)-9) + 'Tables.csv'), 'w') as f:
            writer = csv.writer(f)
            writer.writerows(tab)

# creating zip for every available file
def zipdir(path, ziph, refined_tables):
    for tab in my_tables:
        if my_tables.index(tab) >=9 and my_tables.index(tab)<=109:
            ziph.write(os.path.join('Extracted_csvs', str(my_tables.index(tab)-9) + 'Tables.csv'))
        ziph.write(os.path.join('log_file.log'))

zipf = zipfile.ZipFile('Log_File.zip', 'w', zipfile.ZIP_DEFLATED)
zipdir('/', zipf, my_tables)
zipf.close()
logging.info('csv and log file zipped')

############### Uploading to AWS S3 bucket ###############

server_location = ''

if inputLocation == 'APNortheast':
    server_location = boto.s3.connection.Location.APNortheast
elif inputLocation == 'APSoutheast':
    server_location = boto.s3.connection.Location.APSoutheast
elif inputLocation == 'APSoutheast2':
    server_location = boto.s3.connection.Location.APSoutheast2
elif inputLocation == 'CNNorth1':
    server_location = boto.s3.connection.Location.CNNorth1
elif inputLocation == 'EUCentral1':
    server_location = boto.s3.connection.Location.EUCentral1
elif inputLocation == 'EU':
    server_location = boto.s3.connection.Location.EU
elif inputLocation == 'SAEast':
    server_location = boto.s3.connection.Location.SAEast
elif inputLocation == 'USWest':
    server_location = boto.s3.connection.Location.USWest
elif inputLocation == 'USEast1':
    server_location = boto.s3.connection.Location.USEast1
try:
    time_variable = time.time()
    timestamp_variable = datetime.datetime.fromtimestamp(time_variable)
    bucket_name = AWS_ACCESS_KEY_ID.lower() + str(timestamp_variable).replace(" ", "").replace("-", "").replace(":","").replace( ".", "")
    bucket = conn.create_bucket(bucket_name, location=server_location)
    print("Bucket created")
    zipfile = 'Log_File.zip'
    print("Uploading %s to Amazon S3 bucket %s" %( zipfile, bucket_name))


    def percent_cb(complete, total):
        sys.stdout.write('.')
        sys.stdout.flush()


    k = Key(bucket)
    k.key = 'Log_File_1'
    k.set_contents_from_filename(zipfile,cb=percent_cb, num_cb=10)
    print("Zip File successfully uploaded to S3")
    logging.info("Zip File successfully uploaded to S3")
except Exception as ex:
    print(ex)
    logging.info(ex)
    logging.info("Amazon keys are invalid!!")
    print("Amazon keys are invalid!!")
    exit()
        



