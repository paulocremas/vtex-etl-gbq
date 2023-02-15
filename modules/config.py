import os , ast , json
from modules.CRUD import read
from google.cloud import bigquery
from datetime import datetime , timedelta

global client
global table_id
credentials_path = os.environ['path_google_credentials']
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()
table_id = "sacred-drive-353312.datalakes.orders"

def storesConfig():
    query = read('sacred-drive-353312.config_vtex.storesConfig' , 'TRUE')
    return query

def setConfig(config):
    config = json.loads(config)
    headers = ast.literal_eval(config['headers'])
    url = config['url']    

    setGlobalConfig(headers , url)
    return url

def setGlobalConfig(headersConfig , currentUrl):
    global headers
    global url
    global generalUrl

    headers = headersConfig
    url = currentUrl
    generalUrl = currentUrl

    return

def setStoreName(store):
    global storeName
    storeName = store
    return storeName

#first e last são, respectivamente, de quantos dias atrás até qual dia atrás deve ser puxado
def setExtractionDate(currentUrl , first , last):
    global url
    url = currentUrl + "/api/oms/pvt/orders?f_creationDate=creationDate:%5B{lastdate}T03:00:00.000Z%20TO%20{date}T02:59:59.999Z%5D&orderBy=creationDate,asc&per_page=100&".format(lastdate = str(datetime.today() - timedelta(first))[:10] , date = str(datetime.today() - timedelta(last))[:10])
    url = url + "page={npage}"
    return