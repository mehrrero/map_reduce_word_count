# Map-Reduce for word count

The code in this repository solves the classical word count problem using a strategy map-reduce with an arbitrary number of workers that perform the map and reduce tasks. 

We have a driver server which controls the flow of work, distributing it among the workers and keeping track of completed tasks. A cartoon picture of this can be seen in the figure below.

<img src="./instructions/screenshot1.png" width="50%" alt="Image description">


## Code structure
### Root Directory
- `README.md`
- `driver.py`
- `worker.py`
- `inputs`
- `instructions`
- `intermediate`
- `out`
- `temp`

The `intermediate`, `out` and `temp` folders are created by the driver when executed if they do not exist. `instructions` contains the instructions provided for the test.

### driver.py

This file launches the driver server that controls the workflow. It is built using the `BaseHTTPRequestHandler` of the `http.server` package in python. The `GET` method is used to assing tasks to the workers, while the `POST` methods is used to acknowledge completion. When initiated, it first collects all .txt files in the `inputs` directory and produces a set of $N$ files to be processed (stored in `/temp`), where $N$ is the number of map tasks.

**Parameters:**
- -N (int): Number of map tasks.
- -M (int): Number of reduce tasks
- -p (int): HTTP port for the server to use. The IP will be `localhost:p`.

**Methods:**

- `GET`: A GET call of the form `GET /call` is processed, depending on the value of the handle `call`. Three options are available. If the handle takes the values `map` or `reduce`, the driver checks whether there are remaining tasks of the desired type to be done. If so, it assigns one of them to the worker. If not, it returns a `no_tasks` message.

If the handle is `info`, then the driver returns the total number of tasks of each type.

- `POST`: A POST call signals that a task has been finished. The driver then adds the finished task to the complete_tasks dictionary.

After no more tasks are waiting to be finished, the driver shutsdown, with a waiting time of 40 seconds, just to give time to all workers to receive the `completed tasks` message.

### worker.py

This file runs a worker process which performs the map and reduce tasks received by the driver. It first asks the driver how many tasks of each type are, by a `GET` call. Afterwards, it starts asking for tasks to perform. First, it inquires for map tasks and, if those are not available anymore, for reduce tasks. If all map tasks have been assigned but not finished, it waits before starting to reduce. If there are not task remaining, the worker finishes execution. Once run, the worker waits for a server to be available in the port indicated.

**Parameters:**
- -p (int): HTTP port where the driver is listening. The IP is assumed to be `localhost:p`.

**Tasks:**
- *map task*: Each map task collects a txt file in `temp` and splits all its text into individual words, which are later stored into intermediate bucket files by using the rule `(first letter of the word) % M` to assign the bucket, where $M$ is the number of reduce tasks. The intermediate files can be found in `/intermediate` with names formatted as `mr-<map task id>-<bucket id>.txt`.

- *reduce task*: Each reduce task takes all the files of a given bucket and counts how many times each word appears in them. Then, it produces an output file with the format `out-<reduce task id>.txt` which is stored in the `out` folder.

## Usage:

1. Create an `inputs` folder in the same directory where `driver.py` and `worker.py` are. Put your input files in .txt format in this folder (example files are provided).

2. Run the driver with `python driver.py -N N -M M -p p` where $N,M$ and $p$ are the number of map and reduce tasks, and the port for the HTTP server, respectively.\
       *Example*: `python driver.py -N 5 -M 2 -p 8080` will run the driver with 5 map task, 2 reduce tasks, and over the port 8080.

4. Run any number of instances of the worker with `python worker.py -p p` where $p$ is the port where the driver is listening. Steps 2 and 3 can be executed in any order. \
        *Example*: `python worker.py -p 8080` will run a worker, telling it to expect a driver at port 8080.

Alternatively, we also provide a shell script `map_reduce.sh` which runs all files in a single terminal window. It can be called with optional parameters `-N`, `-M`, `-p`, and `-W` for the number of workers. If any of these parameters are not chosen by the user, the code will run with default values (`N=6, M=4, W=4, p=8080`).



















