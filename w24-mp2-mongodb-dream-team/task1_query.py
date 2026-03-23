'''
Task 1: Step 3: Set a time limit of 120000 sec (2min). Display the requested info
for 4 queries. Each query displays result and time need to run the query.
Step 4: With indices, query 3 runs faster. query 1 is slightly faster (but not significant). 

After running this file once, task1_build.py must be run again in order to see the difference in time between indexed
vs. non-indexed querying. 
'''

import pymongo
import sys
import time

MESSAGES = "messages"
SENDERS = "senders"
TIMELIMIT = 120000

def query1(message_collection):
    '''
    find count of messages with "ant" in their text.
    '''
    
    try:
        s = time.time()
        q1 = {"text": {"$regex": "ant"}}
        a1 = message_collection.count_documents(q1, maxTimeMS=TIMELIMIT)
        f = time.time()
        time_taken = f - s
        print(f"Number of messages that have “ant” in their text: {a1}\nTime taken: {time_taken}")
    except pymongo.errors.ExecutionTimeout:
        print("query1 is timed out as it took >120 000 ms.")
    return

def query2(message_collection):
    '''
    find sender with greater number of messages sent in under 2 min.
    '''
    
    try:
        s = time.time()
        group = {"$group": {"_id": "$sender", "count": {"$sum": 1}}}
        sort = {"$sort": {"count": -1}}
        limit = {"$limit": 1}
        a2 = message_collection.aggregate([group, sort, limit], maxTimeMS=TIMELIMIT)  # returns cursor object
        f = time.time()
        time_taken = f - s
        for doc in a2:
            sender = doc['_id']
            max_messages = doc['count']
        print(f"Sender who has sent the greatest number of messages: {sender} sent {max_messages} messages")
        print(f"Time taken: {time_taken}")
    except pymongo.errors.ExecutionTimeout:
        print("query2 is timed out as it took >120 000 ms.") 
    return

def query3(message_collection, sender_collection):
    '''
    return number of messages where the sender's credit is 0
    1st: find all sender_id in senders.json where credit is 0
    2nd: count_documents in messages.json where sender_id matches sender_id from 1st step
    '''

    try:
        s = time.time()
        '''
        s1 = sender_collection.find({"credit": 0}, {"_id": 0, "sender_id": 1})
        count = 0
        for doc in s1:
            #print(doc["sender_id"])
            #q3 = message_collection.find()
            count += message_collection.count_documents({"sender": doc["sender_id"]})
            #print(count)
            #break
        '''
        
        #lookup = {"$lookup": {"from": "messages", "localField": "sender_id", "foreignField": "sender", "as": "lookup_info"}}
        match = {"$match": {"credit": 0}}
        lookup = {"$lookup": {"from": "messages", "localField": "sender_id", "foreignField": "sender", "as": "messages"}}
        project = {"$project": {"_id": 0, "sender_id:": 1, "message_count": {"$size": "$messages"}}}
        group = {"$group": {"_id": 0, "count":{"$sum": "$message_count"}}}
        a3 = sender_collection.aggregate([match, lookup, project, group], maxTimeMS=TIMELIMIT)
        for i in a3:
            count = i['count']

        f = time.time()
        time_taken = f - s
        print(f"Number of messages where the sender's credit is 0: {count}")
        print(f"Time taken: {time_taken}")
    except pymongo.errors.ExecutionTimeout:
        print("query3 is timed out as it took >120 000 ms.") 
    return


def query4(sender_collection):
    '''
    credit*2 for all senders with credit < 100
    use updateMany()
    '''
    
    s = time.time()
    sender_collection.update_many({"credit": {"$lt": 100}}, {"$mul": {"credit": 2}})
    f = time.time()
    
    time_taken = f - s
    print(f"Credit has has been doubled for those with credit < 100.")
    print(f"Time taken: {time_taken}")
    return

def create_indices(message_collection, sender_collection):
    '''
    create indices for "sender" (default) and "text" (text index) in messages collection.
    Create an index for "sender_id" in senders collection
    '''

    #ASCENDING is default
    message_collection.create_index([("sender", pymongo.ASCENDING)])
    message_collection.create_index([("text", pymongo.TEXT)])  # support text search queries

    sender_collection.create_index([("sender_id", pymongo.ASCENDING)])
    print("Indices created.")
    return


def main(): 
    if len(sys.argv) != 2:
        print("usage: python3 task1_query port")
        sys.exit(1)
    
    port = int(sys.argv[1]) # take a port number from CLI
    client = pymongo.MongoClient("localhost", port) # connect to mongod server
    db = client["MP2Norm"] # create db MP2Norm if it does not exist
    message_collection = db[MESSAGES]
    sender_collection = db[SENDERS]
    query1(message_collection)
    query2(message_collection)
    query3(message_collection, sender_collection)
    query4(sender_collection)

    create_indices(message_collection, sender_collection)

    query1(message_collection)
    query2(message_collection)
    query3(message_collection, sender_collection)

if __name__ == '__main__':
    main()
