'''
Task 2: Step 2: Create an embedded collection named "messages", which contains message and sender information. 
messages.json and senders.json must be in same directory
MongoDB port number is taken in as CL argument.
'''
import json
from pymongo import MongoClient
import sys
import time

def read_senders(file_name):
    with open(file_name, 'r') as file:
        data = json.load(file)
        return data        

def process_messages(messages_file, senders_data, batch_size, messages_collection):

    start = time.time()

    messages_dictionary = []
    with open(messages_file, 'r',  encoding='utf-8') as file:
        for line in file:
            if line != "[\n" and line[len(line)-1] != "]":
                new_line = line.replace("}," , "}")
                new_line = str(new_line).strip()
                message = json.loads(new_line)
                sender_id = message['sender']
                sender_info = next((sender for sender in senders_data if sender['sender_id'] == sender_id), None)
                if sender_info:
                    message['sender_info'] = sender_info
                    messages_dictionary.append(message)
                else:
                    messages_dictionary.append(message)
            if len(messages_dictionary) == batch_size:
                    messages_collection.insert_many(messages_dictionary)
                    messages_dictionary = []
    
        if messages_collection != []:
            messages_collection.insert_many(messages_dictionary)
    
    messages_dictionary = []

    end = time.time()
    time_to_read = end - start
    print("Time to read and create embedded messages collection: " + str(time_to_read))

def main():
    if len(sys.argv) != 2:
        print("Usage: python task2_build.py <port_number>")
        return

    port_number = int(sys.argv[1])
    batch_size = 7500
    
    client = MongoClient(f"mongodb://localhost:{port_number}/")
    db = client["MP2Embdb"]

    # Drop the collection if it exists
    db.drop_collection("messages")

    messages_collection = db["messages"]
    senders_data = read_senders("senders.json")
    messages_file = "messages.json"

    # Process messages in batches and insert into MongoDB
    process_messages(messages_file, senders_data, batch_size, messages_collection)


if __name__ == "__main__":
    main()    
