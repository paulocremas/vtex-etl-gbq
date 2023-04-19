import modules.config as config

#Insere pedidos no BigQuery -- aka CREATE
def insertOrders(orderId , creationDate , status , totalValue , paymentNames , utmSource , utmCampaign , seller , clientName, document , daysSinceLastOrder , repurchaseNumber , repurchaseClient):
    rows_to_insert = [
            {u'orderId': str(orderId) , u'creationDate': str(creationDate) , u'status': str(status) , u'totalValue': str(totalValue) , u'paymentNames': str(paymentNames) , u'utmSource': str(utmSource) , u'utmCampaign': str(utmCampaign) , u'seller': str(seller) , u'clientName': str(clientName),u'clientDocument': str(document),u'platformName' : "VTEX" , u'ecommerceName' :config.storeName , u'daysSinceLastOrder' : daysSinceLastOrder , u'repurchaseNumber' : repurchaseNumber , u'repurchaseClient' : repurchaseClient}
            ]
        
    errors = config.client.insert_rows_json(config.table_id, rows_to_insert)
    if errors == []:
        counter = 1
    else:
        print(f'Encountered errors while inserting rows: {errors}')
    return counter

def insert(insertString):
    query_job = config.client.query("""
        INSERT INTO {} (orderId , creationDate , status , totalValue , paymentNames , utmSource , utmCampaign , seller , clientName , clientDocument , daysSinceLastOrder , repurchaseNumber , repurchaseClient , platformName , ecommerceName)
        VALUES {}
        """.format(config.table_id , insertString))
    orders = query_job.result()
    print("1 order inserted")
    return orders

#lê dados dos big query -- aka READ
def read(table_id , condition):
    query_job = config.client.query("""
        SELECT *
        FROM {}
        WHERE {}
        """.format(table_id , condition))
    orders = query_job.result()
    return orders

#consulta a data do último pedido 
def readRepurchaseData(clientDocument , ecommerceName):
    query_job = config.client.query("""
        SELECT MAX(creationDate) , MAX(repurchaseNumber) , repurchaseClient
        FROM `{}`
        WHERE clientDocument = '{}' AND ecommerceName = '{}'
        GROUP BY repurchaseClient;
        """.format(config.table_id , clientDocument , ecommerceName))
    date = query_job.result()
    return date

#deleta os dados das condições configurada -- aka DELETE
def delete():
    query_job = config.client.query("""
        DELETE FROM {}
        WHERE {};
        """.format(config.table_id , config.interval)) 
    query_job.result()

    return

def update(update , condition , message):
    query_job = config.client.query("""
        UPDATE {}
        SET {}
        WHERE {};
        """.format(config.table_id , update , condition)) 
    query_job.result()

    print(message)

    return

def lastUpdateDate(update):
    query_job = config.client.query("""
        UPDATE `sacred-drive-353312.config_vtex.storesConfig`
        SET lastUpdateDatalake = "{}"
        WHERE store = "{}";
        """.format(update , config.storeName))
    query_job.result()

    print('Update date updataded.')

    return