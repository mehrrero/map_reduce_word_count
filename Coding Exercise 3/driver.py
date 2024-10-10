# driver.py
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
import argparse

# We will take N and M from command line when setting up the driver server, as well as the port address
parser = argparse.ArgumentParser(description='Inputs for N, and the port address.')
parser.add_argument('--N', type=int, required=True, help='Number of map tasks (N).')
parser.add_argument('--M', type=int, required=True, help='Number of reduce tasks (M).')
parser.add_argument('--p', type=int, required=True, help='Port address for the server (p)')
args = parser.parse_args()
N = args.N
M = args.M
p = args.p
    
# We create two dictionaries to track remaining tasks and completed ones. We will do it in list format
tasks = {'map': list(range(N)), 'reduce': list(range(M))}  # Tasks available to assign
completed_tasks = {'map': [], 'reduce': []}  # Tasks that have been completed


#############################################################

class Driver(BaseHTTPRequestHandler):
    def do_GET(self):
        """
        Returns a task type and task ID in JSON format to a request from the worker.
        """
        
        #The task is requested by a call of the form .request('GET', '/map'). Hence, by splitting and calling the last element, we get
            #the kind of task requested.
        task = self.path.split('/')[-1]
        
        id = None #We initialize the variable to None, which will handle finished tasks (see below)
        
        # We assign tasks by extracting the task ID from the dictionary and eliminating it from there, using pop
        if task == 'map' and tasks['map']: # If there are no tasks of a type left, this returns False
            id = tasks['map'].pop(0)  
        elif task == 'reduce' and tasks['reduce']:
            id = tasks['reduce'].pop(0)  
           
            
        # We send the response. 
        if id is not None:
            reply = {'task': task, 'id': id}
        else:
            reply = {'task': 'no_tasks', 'id': id} # If there are no tasks of a kind left, we return 'no_tasks'
        
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
        
        # We read the JSON coming from the worker
        size = int(self.headers['Content-Length'])
        message = json.loads(self.rfile.read(size).decode('utf-8'))
        
        task = message['task']
        id = message['id']
        completed_tasks[task].append(id) # We add the completed task to the dictionary
        
        self.send_response(200)# We signal a succesful request
        self.end_headers()

    #################################


def _driver(port):
    """
    Runs the driver server.

    Parameters (int): Port number that the driver will be using.
    """
    address = ('', port) # We set the address of the driver running in all interfaces.
    server = HTTPServer(address, Driver) #We load the server.
    print(f"Server on port {port}")
    server.serve_forever() # We set the server to be eternally on.

#######################################


def _text_parsercombine_and_split_txt_files(folder_path, num_files):
    """
    Combine all .txt files in the specified folder, preserving spaces,
    and split the combined content into a set of numbered .txt files.

    Parameters:
    folder_path (str): The path to the folder containing .txt files.
    num_files (int): The number of output files to create.

    Returns:
    None
    """
    # Step 1: Gather all .txt files in the specified folder
    txt_files = [f for f in os.listdir(folder_path) if f.endswith('.txt')]
    combined_content = ""

    # Step 2: Read and combine the contents of all .txt files
    for txt_file in txt_files:
        file_path = os.path.join(folder_path, txt_file)
        with open(file_path, 'r', encoding='utf-8') as file:
            combined_content += file.read() + " "  # Preserve spaces

    # Step 3: Calculate the size of each split file
    content_length = len(combined_content)
    split_size = content_length // num_files if num_files > 0 else 0

    # Step 4: Split combined content into the specified number of files
    for i in range(num_files):
        start_index = i * split_size
        # Ensure we don't exceed the content length
        end_index = content_length if i == num_files - 1 else (i + 1) * split_size

        # Create the output file name
        output_file_name = f'output_{i + 1}.txt'
        output_file_path = os.path.join(folder_path, output_file_name)

        # Write the split content to the output file
        with open(output_file_path, 'w', encoding='utf-8') as output_file:
            output_file.write(combined_content[start_index:end_index].strip())

    print(f"Created {num_files} files in '{folder_path}'.")

# We set the main function, which will create the needed temporal folders and run the server

if __name__ == '__main__':

    if not os.path.exists('temp'):
        os.mkdir('temp')
        
    if not os.path.exists('temp/intermediate'):
        os.mkdir('temp/intermediate')

    if not os.path.exists('output'):
        os.mkdir('output')

    if not os.path.exists('temp/tasks'):
        os.mkdir('temp/tasks')
    

    
    _driver(p)  # Driver running on port 8080
