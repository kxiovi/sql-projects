'''
Task 1: Step 1: Create collection named "messages". Data is inserted in small batches. 
The input json is in the same folder as this file. The file name is hard coded.
Port number is inputted.
Step 2: Create collection names "senders". This is not in small batches since the file
is small. The senders file is inserted using json.load().
'''

import pymongo
import json
import sys
import time

MESSAGES = "messages"
MESSAGESJSON = "messages.json"
DATALOADLIMIT = 7999

def messages(db):
    '''
    input file cannot fit in memory, handle the json file without loading the entire file in memory.
    reference for json.loads(): https://stackoverflow.com/questions/12451431/loading-and-parsing-a-json-file-with-multiple-json-objects
    reference reading line by line: https://stackoverflow.com/questions/16573332/jsondecodeerror-expecting-value-line-1-column-1-char-0
    reference for .appends(): https://stackoverflow.com/questions/12451431/loading-and-parsing-a-json-file-with-multiple-json-objects
    '''

    # if messages collection exists, drop it & create a new messages collection
    db.drop_collection(MESSAGES)
    message_collection = db[MESSAGES]
    
    # open messages JSON
    start = time.time()
    data = []
    # counter = 1
    with open(MESSAGESJSON, 'r', encoding='utf-8') as read_file:
        for line in read_file:
            if line != "[\n" and line != "]":
                new_line = line.replace("},","}")
                new_line = str(new_line).strip()
                # print(new_line)
                # above code converts line in read_file into format that can be passed into json.loads()
                data.append(json.loads(new_line))
                # print(data)
                # print(len(data))
            if len(data) > DATALOADLIMIT:
                # print(f"Uploading to DB {(DATALOADLIMIT+1)*counter}")
                # counter+=1
                message_collection.insert_many(data)
                data=[]
        message_collection.insert_many(data) # insert leftover data
    
    data=[] # clear data array
            
    end = time.time()
    print(f"Time to read & create message collection: {end - start}")
    return

def senders(db):
    '''
    create senders collection in MP2Norm. (not in small batches)
    '''
    
    # check if the senders collection exist or not. If yes, drop the collection
    collection_sender = 'senders'
    if collection_sender in db.list_collection_names():
        db[collection_sender].drop()
    
    senders_collection = db["senders"]
    
    # read data from json file
    start_time = time.time()
    with open("senders.json", "r") as file:
        senders_json = json.load(file)
        
    senders_collection.insert_many(senders_json)
    
    end_time = time.time()
    print(f"Time to read & create senders collection: {end_time - start_time}")
    return
        
def main(): 
    if len(sys.argv) != 2:
        print("usage: python3 task1_build port")
        sys.exit(1)
    
    port = int(sys.argv[1]) # take a port number from CLI
    client = pymongo.MongoClient("localhost", port) # connect to mongod server
    db = client["MP2Norm"] # create db MP2Norm if it does not exist
    messages(db)
    senders(db)

if __name__ == '__main__':
    main()

