# driver.py
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
import argparse
import glob
import threading
import time
import shutil

#############################################################
# We build the driver server by using the BaseHTTPRequestHandler class from http.server
class Driver(BaseHTTPRequestHandler):
    def do_GET(self):
        """
        Returns a task type and task ID in JSON format to a request from the worker.
        """

        global completed_tasks, tasks, N, M
        
        #The task is requested by a call of the form .request('GET', '/call'). Hence, by splitting and calling the last element, we get the kind of task requested.
        call = self.path.split('/')[-1]
        
        
        # If the worker asks for info, we send it back
        if call == 'info':
            reply = {'N': N, 'M': M} # Info retrieves M and N
            
        # The other option is that the worker asks for a task
        elif call == 'task': 
        # We assign tasks by extracting the task ID from the dictionary and eliminating it from there, using pop 
            reply = {'task': 'no_tasks', 'id': None} #By default we assign no tasks
            
            if tasks['map']: # If there are no tasks of a type left, this returns False
                task = 'map'
                id = tasks['map'].pop(0) 
                print(f"Sending map task with ID: {id} to worker.")
                reply = {'task': task, 'id': id}
                
            elif tasks['reduce'] and not tasks['map']:
                task = 'reduce'
                if len(completed_tasks["map"])<N:
                    id = -1   # This sends a flag value if maps are not done yet
                else:
                    id = tasks['reduce'].pop(0)
                    print(f"Sending reduce task with ID: {id} to worker.")
                    
                reply = {'task': task, 'id': id}
            
            
                
        # We send the response.         
        self.send_response(200) # We signal a succesful request
        # We will send the reply using a JSON format
        self.send_header('Content-type', 'application/json') 
        self.end_headers()
        self.wfile.write(json.dumps(reply).encode('utf-8'))


    #################################
    def do_POST(self):
        """
        Processes a notification of completed task from a worker.
        """
        global completed_tasks
        
        # We read the JSON coming from the worker
        size = int(self.headers['Content-Length'])
        message = json.loads(self.rfile.read(size).decode('utf-8'))
        
        task = message['task']
        id = message['id']
        if id != -1:
            completed_tasks[task].append(id) # We add the completed task to the dictionary
            print(f"{task} task with ID: {id} is done.")
        self.send_response(200)# We signal a succesful request
        self.end_headers()

    #################################

#######################################
def _driver(port):
    """
    Runs the driver server.
    
    Parameters:
        port (int): Port number that the driver will be using.
    """
    global completed_tasks, M

    address = ('', port)  # We set the address of the driver running on all interfaces.
    server = HTTPServer(address, Driver)  # We load the server.
    
    print(f"Driver running on port {port}.")
    
    # We run the server in a separate thread so we can check for task completion
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True  # We set the server to run independently of the main thread
    thread.start()
    
    # Periodically check if all tasks are completed, then shut down the server
    try:
        while True:
            if len(completed_tasks["reduce"]) == M: # The server closes when all reduce tasks are done
                print("----------------------")
                print("All tasks completed. Shutting down server in 20 seconds.")
                print("----------------------")
                time.sleep(20) # We add a waiting time in case any worker is waiting in the step after asking for reduce
                server.shutdown() 
                break
            time.sleep(2)  # Check every 2 seconds
    except KeyboardInterrupt:
        print("Server interrupted. Shutting down.")
        server.shutdown()

    thread.join()  # Wait for the server thread to finish before finishing this function

#######################################

def process_input_text(path, N):
    """
    Processes all the txt files in the path folder and splits them in
    N_tasks files to send later to the map tasks.

    Parameters:
        path (string): Path of the folder containing the input files.
        N (int): Number of output files.
    
    """

    lines = []
    
    # We start by collecting all the txt files in the given folder and concatenating
        #all their lines
    
    txts = glob.glob(os.path.join(path, '*.txt'))
    
    for file in txts:
        with open(file, 'r') as fi:
            lines.extend(fi.readlines())
    
    total = len(lines)
    
    # We now calculate how many lines to put in each of the files for the tasks
    lines_per_file = total // N
    remainder = total % N  # Some files might need an extra line if the division is not exact

    #We output the files by iterating in N_tasks
    start = 0
    for count in range(1, N + 1):
        # We calculate the number of lines for the current file
        # We add another line to the files until we distribute all the remainder lines
        current_file_lines = lines_per_file + (1 if count <= remainder else 0) 
        end = start + current_file_lines
        
        # We write the lines to a new file
        name = os.path.join("temp/", f'{count-1}.txt')
        with open(name, 'w') as file_i:
            file_i.writelines(lines[start:end])
        
        start = end  # Move the start index for the next file

########################################


# We will take N and M from command line when setting up the driver server, as well as the port address
parser = argparse.ArgumentParser(description='Inputs for N, and the port address.')
parser.add_argument('-N', type=int, required=True, help='Number of map tasks (N).')
parser.add_argument('-M', type=int, required=True, help='Number of reduce tasks (M).')
parser.add_argument('-p', type=int, required=True, help='Port address for the server (p)')
args = parser.parse_args()
N = args.N
M = args.M
p = args.p
    
# We create two dictionaries to track remaining tasks and completed ones. We will do it in list format
tasks = {'map': list(range(N)), 'reduce': list(range(M))}  # Tasks available to assign
completed_tasks = {'map': [], 'reduce': []}  # Tasks that have been completed

# We set the main function, which will create the needed temporal folders and run the server
if __name__ == '__main__':

    """
    Script that initializes the driver server.
    
    Parameters:
    -N (int): Number of map tasks.
    -M (int): Number of reduce tasks.
    -p (int): Port on which the server will be listening.
    """

    # We first create the required folders. We delete first older versions to avoid problems with files with similar names
    if os.path.exists('temp'):
        shutil.rmtree('temp')
    os.mkdir('temp')
    
    if os.path.exists('intermediate'):   
        shutil.rmtree('intermediate')
    os.mkdir('intermediate')
    
    if os.path.exists('out'):
        shutil.rmtree('out')
    os.mkdir('out')


    process_input_text("inputs/", N) # We start by processing the input test to generate N map tasks.
    print(f"Running map_reduce with N={N} map tasks and M={M} reduce tasks.")
    print("Output files can be found in /out.")
    _driver(p)  # Driver running on port p
