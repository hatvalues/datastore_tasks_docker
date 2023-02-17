import os
import multiprocessing as mp

def async_work(i, arg1, arg2): 
    # the i is like a sequence number
    # use it if results should be sorted in order they started
    try:
        # whatever you need to actually do goes here
        # essential to catch errors for later because they are horrible to debug inside thread pool
        out = arg2[i]
        Error = None
    except KeyError:
        out = {}
        Error = None
    except Exception as Error:
        out = {}
    return(i, out, arg1, arg2, Error)

# set up mp pool
n_cores = os.cpu_count() - 2 # leave some cores for the OS
async_out = [] # to collect results
pool = mp.Pool(processes=n_cores)

my_files = ["file1", "file2", "file3", "file4"]

for i, my_file in enumerate(my_files):
    # creates the jobs
    async_out.append(pool.apply_async(async_work, (i, my_file, my_files))) # tuple requred for all the arguments

# block the pool and collect the completed pool
pool.close()
pool.join()

# get the async results and sort to ensure original order
gather = [async_out[j].get() for j in range(len(async_out))]
gather.sort() # this will sort by the first member of the output tuple

errors = [e for e in gather if e[4]] # in this template, errors are in position 2. Hopefull all Nones.
if errors:
    print("do something with the errors")

# You can get all the values for one output argument in the usual way
results = [gather[k][1] for k in range(len(gather))]

# here we expect "out" to be the same as "arg1"
for result, my_file in zip(results, my_files):
    print(result, my_file)