import os
import multiprocessing as mp
from google.cloud import datastore
from utilities import get_entity_keys, StandardLogger

def async_work(key, arg1, arg2):
    """Defines work to be done in a multi processing step.
    
    All args are optional, depending on the use case.
    Names of args should change to be more descriptive of purpose
    Args:
        key: example, unique id if working with identifiable objects
        arg1: example, enumerated id of task if results need sorted after completion
        arg2: any other value needed for op.
    Returns:
        all input args can be returned for debugging
        be sure to return any error messages
    Raises:
        n/a - trap any errors and return as messages for debugging after. 
    """
    datastore_client_mp = datastore.Client()
    entity = datastore_client_mp.get(key)
    try:
        id = entity["id"]
        name = entity["name"]
        Error = None
    except KeyError:
        id = key.id
        name = "missing"
        Error = None
    except Exception as Error:
        id = key.id
        name = "something is wrong"
    # do something useful here e.g.
    # entity.update({"key" : "value"})
    # datastore_client_mp.put(entity)
    return(id, name, Error, arg1, arg2)

# prevent multiple instances of the log file handler
if not "lg" in globals():
    lg = StandardLogger("multiprocessing_example")

lg.clear_log()

lg.log_print("info", "STARTING JOB.")

# set up mp pool
n_cores = os.cpu_count() - 2 # leave some cores for the OS
async_out = []
pool = mp.Pool(processes=n_cores)

# logger.log_print(f"started pool, starting loop", "info")
kind = "school"
lg.log_print("info", f"Fetching keys from kind {kind}. Please wait")
entities = get_entity_keys(kind=kind)
lg.log_print("info", f'There are {len(entities)} entities returned from kind {kind} in GCP Project {os.getenv("DATASTORE_PROJECT_ID")}')

lg.log_print("info", f'Creating pool of {n_cores} processes. Watch on System Monitor.')
for i, entity in enumerate(entities):
    # creates the jobs
    async_out.append(pool.apply_async(async_work, (entity.key, i, "arg2"))) # tuple requred for all the arguments

# block the pool and collect
pool.close()
lg.log_print("info", f'Launching pool')
pool.join()

# get the async results and sort by entity key to ensure original tree order and remove tree index
lg.log_print("info", f'Collecting results')
gather = [async_out[j].get() for j in range(len(async_out))]
gather.sort() # this will sort by the first member of the output tuple, which is the key.id in this template
lg.log_print("info", f'There are {len(gather)} results')
errors = [e for e in gather if e[2]] # in this template, errors are in position 2. Hopefull all Nones.
if errors:
    lg.log_print("error", f'There are {len(errors)} errors.')
    lg.log_print("error", f'{errors}')
lg.log_print("info", "FINISHED JOB.")

# You can get all the values for one output argument in the usual way
results = [gather[k][1] for k in range(len(gather))]
print(results)