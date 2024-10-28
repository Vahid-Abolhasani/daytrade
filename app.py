import requests,json,pymongo,time
import xml.etree.ElementTree as et
from pymongo import MongoClient
import concurrent.futures
import pytz
import time as tm
from datetime import datetime 
from datetime import time as tms




client = MongoClient("mongodb://admin:uiAfWzvnNH1UqF3A2qnt@remote-asiatech.runflare.com:30772/boursedata-olt-service?authSource=admin")
mydb = client["bourse"]
mycol = mydb["data of namd"]
sandogh=mydb['idsandogh']

mydbb=client["day_trade"]
mycoll=mydbb["live_data_trade_060102"]
id_sandogh=[]
for doc in sandogh.find():
    id_sandogh.append(doc["id"])


import requests,concurrent.futures,zlib,pymongo
import xml.etree.ElementTree as et

mycol = client["bourse"]["data of namd"]
mycoll = client["day_trade"]["live_data_trade_060102"]

HEADERS = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive'
        }

documents = mycol.find()
document_tuples = []
for document in documents:
    document_tuple = (document['_id'], document['name'])
    document_tuples.append(document_tuple)

def check_time_run(current_time):
    from datetime import datetime, timedelta

    # Define the time string
    time_string = str(current_time)

    # Create a datetime object with today's date and the given time
    datetime_object = datetime.strptime(time_string, "%H:%M:%S.%f")

    # Calculate the total seconds
    total_seconds = timedelta(hours=datetime_object.hour,
                              minutes=datetime_object.minute,
                              seconds=datetime_object.second,
                              microseconds=datetime_object.microsecond).total_seconds()

    # Print the total seconds
    if total_seconds>33480:
        return(86300-int(total_seconds)+33480)#64800=09:00 and 86300 for 24h sec 
    else:
        return(abs(int(total_seconds)-33480))#64800=09:00 and 86300 for 24h sec 

def find_name_by_id(document_tuples, target_id):
    # Iterate over the document tuples and search for the target _id
    for document_tuple in document_tuples:
        if document_tuple[0] == target_id:
            # If the target _id is found, return the corresponding name
            return document_tuple[1]

def compress_data(data):
    return zlib.compress(bytes(str(data), 'utf-8'))

data_1 = [x['_id'] for x in mycol.find() if x['_id'] not in id_sandogh[0]]

def process_id(idd):
    result = get_daytrade_data(idd, session)
    if result is not None:
        mycoll.update_one({"_id": result["_id"]}, {"$set": {"daytrade": result["daytrade"]}})
    else:
        iderror.add(idd)    
tehran_tz = pytz.timezone('Asia/Tehran')
start_time =tms(9, 10,10)  # 9:45 AM
end_time =tms(23,59)  # 13:30 PM
while True:
    
    
    current_time = datetime.now(tehran_tz).time()
    import datetime as dtime
    current_day = dtime.datetime.now().weekday()
    if current_day != 4 and current_day != 3 :
          




            def get_daytrade_data(idd, session):
                url = f'http://old.tsetmc.com/tsev2/data/TradeDetail.aspx?i={str(idd)}'
                response = requests.get(url, headers=HEADERS,timeout=10)
                response.raise_for_status()
                html = et.fromstring(response.content)
                pdd = []
                for row in html.iter('cell'):
                    pdd.append(row.text)
                try:

                    
                    rows, times, volumes, prices = [], [], [], []
                    for i in range(0, len(pdd), 4):
                        rows.append(pdd[i])
                        times.append(pdd[i + 1])
                        volumes.append(pdd[i + 2])
                        prices.append(pdd[i + 3])
                    if len(rows) == 0:
                        return None
                    results = { 'data': []}
                    for i in range(len(rows)):
                        results['data'].append({
                            'row':rows[i],
                            'time': times[i],
                            'volume': volumes[i],
                            'price': prices[i]})

                    return {"_id": idd,'name':find_name_by_id(document_tuples, idd), "daytrade": results['data']}

                except:
                    return None




            with requests.Session() as session:



                iderror = set()


                with concurrent.futures.ThreadPoolExecutor() as executor:
                    for idd in data_1:
                        if idd not in iderror:
                            executor.submit(process_id, idd)


                if len(iderror)<200:
                    print(current_time)
                    print(len(iderror))
                    print('-------')
                    time.sleep(120)
        
    else:
        import time as tmss
        print('tmss')
        tmss.sleep(25200)                        
                            
        
