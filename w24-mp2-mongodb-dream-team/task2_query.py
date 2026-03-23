import pymongo
import sys
import time

TIMELIMIT = 120000

def query1(message_collection):
    try:
        s = time.time()
        q1 = ({"text": {"$regex": "ant", "$options": "m"}})
        a1 = message_collection.count_documents(q1, maxTimeMS=TIMELIMIT)
        f = time.time()
        time_taken = f - s
        print(f"Number of messages that have “ant” in their text: {a1}\nTime taken: {time_taken}")
    except pymongo.errors.ExecutionTimeout:
        print("query1 is timed out as it took >120 000 ms.")
    return

def query2(message_collection):
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

def query3(message_collection):

    try:
        s = time.time()
        count = message_collection.count_documents({"sender_info.credit": {"$eq": 0}})
        f = time.time()
        time_taken = f - s
        print(f"Number of messages where the sender's credit is 0: {count}")
        print(f"Time taken: {time_taken}")
    except pymongo.errors.ExecutionTimeout:
        print("query3 is timed out as it took >120 000 ms.") 
    return


def query4(message_collection):
    
    try:
        s = time.time()
        message_collection.update_many({"sender_info.credit": {"$lt": 100}}, {"$mul": {"sender_info.credit": 2}})
        f = time.time()
        time_taken = f - s
        print(f"Credit has has been doubled for those with credit < 100.")
        print(f"Time taken: {time_taken}")
    except:
        print("query4 timed out as it took >120 000 ms.") 
    return



def main(): 
    if len(sys.argv) != 2:
        print("usage: python3 task2_query port")
        sys.exit(1)
    
    port = int(sys.argv[1]) 
    client = pymongo.MongoClient("localhost", port) 
    db = client["MP2Embdb"]
    message_collection = db["messages"]
    query1(message_collection)
    query2(message_collection)
    query3(message_collection)
    query4(message_collection)
    

if __name__ == '__main__':
    main()
