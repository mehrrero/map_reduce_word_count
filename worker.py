# worker.py
import http.client
import json
import os
import time
import argparse


############################################
def _map(task_id, M):
    """
    Performs the map task. Saves the intermediate file to disk with a filename mr-{task_id}-{bucket_id}.txt.

    Parameters:
        task_id (int): ID of the task to be performed.
        M (int): Number of total reduce tasks.        
    """
    
    input = f'temp/{task_id}.txt'
    
    with open(input, 'r') as f:
        # We eliminate question marks, commas, etc... (Surprisingly, this is the most efficient way in terms of time (?)
        symbols = [";",".",",",":","'",'"',"[","]","{","}","(",")","!","?","#","-","_","*"]

        text = f.read()
        for char in symbols:
            text = text.replace(char, " ")
            
        text = text.split()
    
    # Bucket words by the first letter modulo M
    buckets = [""]*M
    for word in text:
        #We do not distinguish uppercase or lowercase. We also remove spaces at the beggining and end
        word = word.lower().strip()
        
        if word != '': # We add this to solve issues with empty words after eliminating non-alfanumeric characters
        # We set the buket ID using the unicode code for the first character. 
            bucket_id = ord(word[0]) % M 
            buckets[bucket_id] += f"{word}\n"

    # We write the words to the intermediate file 
    for i in range(M):
        intermediate = f'intermediate/mr-{task_id}-{i}.txt'
        with open(intermediate, 'a') as bucket:
            bucket.write(buckets[i]) 
           


###########################################################


def _reduce(bucket_id, N):

    """
    Performs the reduce task for the given bucket_id and all the maps.

    Parameters:
        bucket_id (int): ID of the bucket on which the function performs the reduce task.
        N (int): Number of map tasks that have produced buckets.
    """

    counts = {} # Empty dictionary to store the count

    # We run over the map tasks to collect all bucket files
    for map_id in range(N): 
        intermediate = f'intermediate/mr-{map_id}-{bucket_id}.txt'
        
        if not os.path.exists(intermediate):
            continue # with this we ensure that ignore the file if the map has not generated it
       
        with open(intermediate, 'r') as f:
            for word in f:
                word = word.strip()
                counts[word] = counts.get(word, 0) + 1 # We add one to the word count in the dictionary
    
    # Write the output to a file
    output = f'out/out-{bucket_id}.txt'
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
    # We include the call in a loop so that it keeps trying in case there is some communication problem.
    # Since we have added a first info request, this should not happen, but we keep the insurance loop here, just in case.
    # We added a timeout at the end to space the calls to the driver.

    print("Connecting to the driver to request a task.")
    
    while True:
        try:
            driver = http.client.HTTPConnection(driver_IP) # We connect to the driver
    
            driver.request('GET', '/task')  # We ask the driver for a task
    
            # We obtain the reply from the driver and read it in JSON format 
            reply = driver.getresponse() 
            reply_json = json.loads(reply.read()) 

            driver.close()
            return reply_json

        except (http.client.HTTPException, ConnectionRefusedError) as _:
            time.sleep(2)  # Wait for 2 seconds before retrying
           
    
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

def _info(driver_IP):
    """
    Ask the driver for the values of M and N.
    
    Parameters:
        driver_IP: IP of the driver.
    """
    global M, N, p
    print(f"Trying to connect to driver on port {p}.")
    
    while True: #This runs in an infinite loop to wait for the driver
        try:
            driver = http.client.HTTPConnection(driver_IP) # We connect to the driver
    
            driver.request('GET', '/info')  # We first ask the driver for info
    
            # We obtain the reply from the driver and read it in JSON format 
            reply = driver.getresponse() 
            reply_json = json.loads(reply.read()) 
            M = reply_json["M"]
            N = reply_json["N"]
            driver.close()

            print("Driver found. Starting works.")
            
            return True
            
        except (http.client.HTTPException, ConnectionRefusedError) as _:
            
            time.sleep(2)  # Wait for 2 seconds before repeating.
       
  

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
        
        # Otherwise we retrieve the id and tell the worker to perform the adequate task.
        id = reply['id']
        
        if task == 'map':
            print(f"Performing map task with ID: {id}.")
            _map(id, num_reduces)
            
        elif task == 'reduce':
            if id == -1: # A negative id indicates the worker to wait before retrying to do a reduce task.
                print("All map tasks have been assigned. Waiting for them to finish before starting reduce tasks.\nWorker sleeping for 10 seconds.")
                time.sleep(10) 
            else:
                print(f"Performing reduce task with ID: {id}.")
                _reduce(id, num_maps)
                
        # We end up by letting the driver know that the job is finished.
        _done(driver_host, task, id)
        
###########################################################

# We read p from command line
parser = argparse.ArgumentParser(description='Input for the port address of the driver.')
parser.add_argument('-p', type=int, required=True, help='Port address for the driver.')
args = parser.parse_args()
p = args.p

if __name__ == '__main__':
    """
    Script that runs the worker performing the tasks.
    
    Parameters:
    -p (int): Port on which the driver is listening.
    """

    
    driver_host = f'localhost:{p}'
    M = None
    N = None
    
    _info(driver_host) # We first retrieve the value of N and M from the server.

    Worker(driver_host, N, M)



