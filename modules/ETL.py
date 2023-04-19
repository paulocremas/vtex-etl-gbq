import requests
import modules.config as config
from datetime import datetime , timedelta
from modules.CRUD import insert , read, update , readRepurchaseData , lastUpdateDate
from collections import defaultdict

#Lê cada página e retorna uma lista com os pedidos concatenados de todas as páginas
def extractOrders():
    page = 1
    orderCount = 100
    completeorders = []

    while orderCount == 100:
        url = config.url.format(npage = page)
        orders = requests.get(url, headers=config.headers)
        orders = orders.json()
        orders = orders['list']
        completeorders.extend(orders)
        page = page + 1
        orderCount = len(orders)

    print('Data was successfully extracted.')
    return completeorders

#Faz a tratativa de pedido por pedido
def treatOrdersInsertion(orders):
    counter = 0
    completeorders = []
    repurchaseUpdateList = []
    for order in orders:
        if str(order['marketPlaceOrderId']) == 'None' and str(order['salesChannel']) == '1':
            counter = counter + 1

            info = getInfo(order['orderId'])
            if info['utmSource'] == None or info['utmSource'] == "None":
                utmSource = 'Null'
            else:
                utmSource = info['utmSource']
            if info['utmCampaign'] == None or info['utmCampaign'] == "None":
                utmCampaign = 'Null'
            else:
                utmCampaign = info['utmCampaign']
            if info['seller'] == None or info['seller'] == "None":
                seller = 'Null'
            else:
                seller = info['seller']
            document = info['document']

            order['orderId'] = str(order['orderId'])[:-3]

            order['orderId'] = str(order['orderId']) + "-" + config.storeName
            document = str(document) + " - " + config.storeName

            order['creationDate'] = order['creationDate'][:19].replace('T' , ' ')
            #na próxima linha são removidas 3 horas pois o servidor da VTEX está 3 horas 
            order['creationDate'] = str(datetime.strptime(order['creationDate'], "%Y-%m-%d %H:%M:%S") - timedelta(hours=3))


            #conta a quantidade de dias desde o ultimo pedido
            repurchaseNumber = 'Null'
            daysSinceLastOrder = 'Null'
            repurchaseClient = 'Null'

            repurchaseData = readRepurchaseData(document , config.storeName)
            for repurchase in repurchaseData:
                try:
                    daysSinceLastOrder = datetime.strptime(order['creationDate'], '%Y-%m-%d %H:%M:%S') - repurchase.f0_
                    daysSinceLastOrder = daysSinceLastOrder.days
                except:
                    daysSinceLastOrder = 'Null'

                if repurchase.f1_ is None:
                    repurchaseNumber = 0
                else:
                    repurchaseNumber = int(repurchase.f1_) + 1
                    repurchaseClient = True
                    if repurchase.repurchaseClient is None:
                        repurchaseUpdateList.append(document)


            order['totalValue'] = str(order['totalValue'])[:-2] #remove o .0 do valor
            order['totalValue'] = str(order['totalValue'])[:-2] + '.' + str(order['totalValue'])[-2:] #separa reais de centavos

            data_set = [{"orderId": order['orderId'] ,
                        "creationDate": order['creationDate'] ,
                        "status": order['status'] ,
                        "totalValue": order['totalValue'] ,
                        "paymentNames": order['paymentNames'] ,
                        "utmSource" : utmSource ,
                        "utmCampaign" : utmCampaign ,
                        "seller" : seller ,
                        "clientName" : order['clientName'] ,
                        "document" : document ,
                        "daysSinceLastOrder" : daysSinceLastOrder ,
                        "repurchaseNumber" : repurchaseNumber ,
                        "repurchaseClient" : repurchaseClient
                        }]

            completeorders.extend(data_set)
    print('Data was successfully treated. ' + str(counter) + ' orders.')
    if repurchaseUpdateList:
        update("repurchaseClient = True" , "clientDocument IN ({})".format(str(repurchaseUpdateList)[1:-1]) , "Repurchase status updated.")
    else:
        print("No recurrent clients to uptade.")
    return completeorders

#this part is responsible for getting more specific information about each order, its called only for insertions
def getInfo(orderId):
    orderUrl = config.generalUrl + '/api/oms/pvt/orders/' + orderId
    order = requests.get(orderUrl, headers=config.headers)
    order = order.json()

    try:
        sellerName = str(order['openTextField'])[24:]
        sellerName = sellerName[:-18]
        sellerNumber = str(order['openTextField'])[-7:]
        sellerNumber = sellerNumber[:5]
        seller = sellerName + ' - ' + sellerNumber
    except:
        seller = 'Null'

    seller = seller[3:]

    try:
        return {'utmSource': order['marketingData']['utmSource'],'utmCampaign': order['marketingData']['utmCampaign'], 'seller' : seller , 'document' : order['clientProfileData']['document']}
    except:
        return {'utmSource': 'Null' ,'utmCampaign': 'Null' , 'seller' : seller , 'document' : order['clientProfileData']['document']}

def treatOrdersUpdate(orders):
    completeorders = []
    counter = 0
    for order in orders:
        if str(order['marketPlaceOrderId']) == 'None':
            counter = counter + 1
            order['orderId'] = str(order['orderId'])[:-3]
            data_set = [{"orderId": order['orderId'] , "status": order['status']}]
            completeorders.extend(data_set)
    print('Data was successfully treated.')
    return completeorders

def loadList(orders):
    lenCounter = 0
    counter = 0
    try:
        lenOrders = len(orders)
    except:
        lenOrders = 0
    insertString = ""
    print('Loading data into Big Query.')
    for order in orders:
        counter = counter + 1
        lenCounter = lenCounter + 1
        insertString = insertString + '( "' + str(order['orderId']) +'" , "'+ str(order['creationDate']) +'" , "'+ str(order['status']) +'" , '+ str(order['totalValue']) +' , "'+ str(order['paymentNames']) +'" , "'+ str(order['utmSource']) +'" , "'+ str(order['utmCampaign']) +'" , "'+ str(order['seller']) + '" , "' + str(order['clientName']) +'" , "' + str(order['document']) +'" , '+ str(order['daysSinceLastOrder']) +' , '+ str(order['repurchaseNumber']) +' , '+ str(order['repurchaseClient'])+' , '+ '"VTEX"' +' , "'+ config.storeName + '" )'
        if lenCounter != len(orders):
            insertString = insertString + " , "
    if insertString == "":
        print("No insertions to execute.")
        lastUpdateDate(str(datetime.today() - timedelta(1))[0:10])
    else:
        insert(insertString)
        print('Data was successfully loaded.')
        lastUpdateDate(str(datetime.today() - timedelta(1))[0:10])
    
    return lenOrders

def newOrders(counter):
    newOrders = ""
    while counter > 0:
        config.setExtractionDate(config.generalUrl , counter , counter - 1)
        orders = treatOrdersInsertion(extractOrders())
        newOrders = str(loadList(orders))
        print(newOrders + ' New orders')
        counter = counter - 1
        print(counter)
    return newOrders

def updateOrders():
    config.setExtractionDate(config.generalUrl , 15 , 1)
    vtexOrders = treatOrdersUpdate(extractOrders())
    orderId = ''
    orderUpadateList = defaultdict(list)
    statusList = []
    #cria a consulta com os parametros dos chamados da vtex
    for vtexOrder in vtexOrders:
        orderId = orderId + str("'{}' , ".format(vtexOrder['orderId']))
    orderId = orderId[:-3]
    readCondition = "orderId IN ({}) AND ecommerceName = '{}'".format(orderId,config.storeName)

    #Consulta os chamados o bigquery de acordo com os pedidos da vtex
    try:
        bqOrders = read(config.storeName , readCondition)
        for bqOrder in bqOrders:
            #essa linha filtra e cria uma lista com apenas 1 item, aquele que corresponde ao orderId do bigquery
            vtexOrder = list(filter(lambda x:x["orderId"]==str(bqOrder.orderId),vtexOrders))
            status = str(vtexOrder[0]['status'])
            if (bqOrder.status != status):
                test = str(status) in orderUpadateList
                if (test == False):
                    #cria a chave dentro do json
                    orderUpadateList[status] = [bqOrder.orderId]
                    #cria lista de status
                    statusList.append(status)
                else:
                    #adiciona orderId na chave criada acima
                    orderUpadateList[status].append(bqOrder.orderId)

        for status in statusList:
            updateCondition = "orderId IN ({})".format(str(orderUpadateList[status])[1:-1])
            update("status = '" + status + "'", updateCondition , str("Orders status uptade to: " + status))
    except:
        print('sem pedidos para atualizar')
    return






