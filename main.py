import time
from modules.ETL import newOrders , updateOrders
import modules.config as configuration
from modules.emailsender import sendEmail
from datetime import datetime as dt

def run():
    main_time = time.time()
    storeCounter = 0
    newOrdersCounter = 0
    storesConfig = configuration.storesConfig()

    for config in storesConfig:
        print('------------------------')
        print(configuration.setStoreName(config.store))
        configuration.setConfig(config.data)
        
        days = dt.today() - config.lastUpdateDatalake
        days = days.days
        days = days - 1
        newOrdersCounter = newOrdersCounter + int(newOrders(days))
        storeCounter = storeCounter + 1
        updateOrders()
    processTime = time.time() - main_time
    
    if newOrdersCounter != 0:
        sendEmail("""<p>Lojas VTEX - DATALAKE atualizadas com sucesso: {}/5 </p>
        <p>Total de pedidos inseridos no Big Query: {} </p>
        <p>Tempo de processo: {} segundos</p>""".format(storeCounter,newOrdersCounter,processTime).encode('utf-8'))

    return

run()
print("Complete.")