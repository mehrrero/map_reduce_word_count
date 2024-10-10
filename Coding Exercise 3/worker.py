# worker.py
import http.client
import json
import os
import time

def _map(task_id, num_reduces):
    """
    Performs the map task. Saves the intermediate file to disk with a filename mr-{task_id}-{bucket_id}.txt.

    Parameters:
        task_id (int): ID of the task to be performed.
        num_reduces (int): Number of total reduce tasks.        
    """
    input = f'temp/tasks/{task_id}.txt'
    with open(input, 'r') as f:
        text = f.read().split()
    
    # Bucket words by the first letter modulo M
    for word in text:
        
        #We do not distinguish uppercas or lowercase
        word = word.lower()
        
        # We set the buket ID using the unicode code for the first character. 
        bucket_id = ord(word[0].lower()) % num_reduces 

        # We append the words to the intermediate file 
        intermediate = f'temp/intermediate/mr-{task_id}-{bucket_id}.txt'
        with open(intermediate, 'a') as bucket:
            bucket.write(f"{word}\n")


###########################################################


def _reduce(bucket_id, num_maps):

    """
    Performs the reduce task for the given bucket_id and all the maps.

    Parameters:
        bucket_id (int): ID of the bucket on which the function performs the reduce task.
        num_maps (int): Number of map tasks that have produced buckets.
    """

    counts = {} # Empty dictionary to store the count

    # We run over the map tasks to collect all bucket files
    for map_id in range(num_maps): 
        intermediate = f'temp/intermediate/mr-{map_id}-{bucket_id}.txt'
        if not os.path.exists(intermediate):
            continue
        with open(intermediate, 'r') as f:
            for word in f:
                word = word.strip() # We remove spaces in and after the word, just in case
                counts[word] = counts.get(word, 0) + 1 # We add one to the word count in the dictionary
    
    # Write the output to a file
    output = f'output/out-{bucket_id}.txt'
    with open(output, 'w') as f:
        for word, count in counts.items():
            f.write(f"{word} {count}\n")

###########################################################

def _request(driver_IP):
    """
    Requests a task to the driver.

    Parameters:
        driver_IP (string): IP address of the driver, eg. 'localhost:8000'.

    Returns:
        reply_json (JSON): File with JSON data of the task requested.
    """
    # We include the call in a loop so that it keeps trying in case there is no driver server yet on. 
    # We added a timeout at the end to space the calls to the driver.
    tries = 1
    while tries < 10: # We close the loop after 10 un-succesful tries (100 seconds)
        try:
            driver = http.client.HTTPConnection(driver_IP) # We connect to the driver
    
            driver.request('GET', '/map')  # We first ask the driver for a map task
    
            # We obtain the reply from the driver and read it in JSON format 
            reply = driver.getresponse() 
            reply_json = json.loads(reply.read()) 
    
            # Checks if the map tasks are done. If so, asks for a reduce task
            if reply_json['task'] == 'no_tasks':
                driver.request('GET', '/reduce')
                reply = driver.getresponse()
                reply_json = json.loads(reply.read())

            driver.close()
            return reply_json

        except (http.client.HTTPException, ConnectionRefusedError) as _:
            print("Driver not found. Retrying in 4 seconds")
            time.sleep(4)  # Wait for 4 seconds
            tries += 1

    print("Driver not found after 10 attempts")
    return {'task': 'timeout'} # We return a finished task to close the worker
    
###########################################################


def _done(driver_IP, task, id):

    """
    Notifies the driver that the task is done.

    Parameters:
        driver_IP (string): IP address of the driver, eg. 'localhost:8000'.
        task (string): Task that has been performed (map or reduce).
        id: ID of the task done.
        
    """
    
    connection = http.client.HTTPConnection(driver_IP) # We connect to the driver

    #We save the output in a json file that will be sent to the server through the POST method
    head = {'Content-type': 'application/json'}
    info = json.dumps({'task': task, 'id': id})
    connection.request('POST', '/', info, head)
    connection.getresponse()
    connection.close()
    
############################################################

def Worker(driver_IP, num_maps, num_reduces):
    """
    Function that runs the worker in a loop.

    Parameters:
        driver_IP (string): IP address of the driver, eg. 'localhost:8000'.
         num_maps (int): Number of map tasks. 
         num_reduces (int): Number of reduce tasks.
    """
    
    while True: #We set an infinite loop so that the worker continuously request the driver for work to do.
        
        reply = _request(driver_IP) # We request a task. Depending on the type we do one of another thing.
        task = reply['task']
        
        if task == 'no_tasks':
            # If there are no tasks available, we turn off the worker by breaking the loop.
            print("All tasks are done. Shutting down the worker.")
            break
            
        if task == 'timeout':
            # We add a message in case the server has timeout'd
            print("No driver server available. Shutting down worker.")
            break
        
        # Otherwise we retrieve the id and tell the worker to perform the adequate task.
        id = reply['id']
        
        if task == 'map':
            print(f"Map {id}")
            _map(id, num_reduces)
        elif task == 'reduce':
            print(f"Reduce {id}")
            _reduce(id, num_maps)
        
        # We end up by letting the driver know that the job is finished.
        _done(driver_host, task, id)

###########################################################



if __name__ == '__main__':
    driver_host = 'localhost:8086'
    num_maps = 3
    num_reduces = 3
    Worker(driver_host, num_maps, num_reduces)