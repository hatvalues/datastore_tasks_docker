import pickle
import os
from utilities import query_filters, StandardLogger

class CursorHandler:
    """A class to manage the datastore cursor based task.
    Call from a cursor handler module, template in cursor handler example.py

    ...
    Methods:
    -------
    get_entities_by_cursor(name: str, kind: str, filters : dict, limit : int, operation : dict)
        Note the arguments are unpacked kwargs from the task dictionary, given in example file
        Handles the bulk management of getting entities at in bulk and saving the cursor position.
        The cursor is written/read to a binary file for working manually outside of the loop,
        There is some disk i/o but it is tiny - cursor is just a few bytes

    run_task(entities : list of datastore.Entity)
        Executes the operation lambda on each entity

    run_cursor()
        Manages initialising a new cursor, and opening and closing the cursor state.
    """
    def __init__(self, task) -> None:
        if set(task.keys())=={'name', 'kind', 'filters', 'limit', 'operation'}:
            self.task = task
            self.cursor_file = f'{self.task["name"]}.pickle'
            self.logger = StandardLogger(task["name"])
            self.environment = "emulator" if os.getenv("DATASTORE_EMULATOR_HOST") else "datastore"
        else:
            raise ValueError("task must be a dictionary with the following keys: 'name', 'kind', 'filters', 'limit', 'operation'")

    def get_entities_by_cursor(self, kind="(empty)", filters={}, limit=10, cursor=None):
        query = query_filters(kind, filters, projection=[]) # implementing projections seems a bit unnecessary for the moment
        # start_cursor keyword provides a cursor object in the return value
        query_iter = query.fetch(start_cursor=cursor, limit=limit)
        entities = list(next(query_iter.pages))
        next_cursor = query_iter.next_page_token
        self.logger.log_print("info", f"{len(entities)} results for kind {kind} with filters {filters} and limit {limit}.")
        self.logger.log_print("info", f"Here is the next cursor: {next_cursor if next_cursor else '(empty)'}")
        return(entities, next_cursor) # this is the new cursor position

    def run_task(self, entities):
        self.logger.log_print("info", f'Running task {self.task["name"]} with on {len(entities)} collected by cursor.')
        successes = [False] * len(entities)
        for i, entity in enumerate(entities):
            try:
                self.task["operation"](entity)
                successes[i] = True
                self.logger.log_print("info", f'Task {self.task["name"]} completed successfully on entity {entity.id}')
            except Exception as E:
                self.logger.log_print("info", f'Task {self.task["name"]} failed on entity {entity.id} with error: {E}')
        return successes

    def run_cursor(self, first_run=False):
        # clear a previous run, if necessary
        if first_run:
            self.logger.log_print("info", "First run. Deleting previously saved cursor.")
            if os.path.isfile(self.cursor_file):
                os.remove(self.cursor_file)
        # regular run, needs to check if a previous cursor is there
        if os.path.isfile(self.cursor_file):
            self.logger.log_print("info", "Opening previously saved cursor.")
            with open(self.cursor_file,"rb") as f:
                next_cursor = pickle.load(f)
            self.logger.log_print("info", f"Here is the cursor: {next_cursor if next_cursor else '(empty)'}")
        else:
            # if not, start from the beginning with a null cursor input
            self.logger.log_print("info", "Starting new cursor.")
            next_cursor = None
        # here do the work
        entities, next_cursor = self.get_entities_by_cursor(kind=self.task["kind"],
                                                            filters=self.task["filters"],
                                                            limit=self.task["limit"],
                                                            cursor=next_cursor)
        # This logic based on learning from cursor loop test. Emulator does not behave as expected at end of cursor
        if self.environment=="emulator" and not entities: # hits the end of the data set because cursor position doesn't exist in the kind
            self.logger.log_print("info", "Reached end of the data set. Done.")
            return(False)
        elif self.environment=="datastore" and not next_cursor: # end of the dataset but need to complete the tail. cursor is None and don't want to start from the beginning
            successes = self.run_task(entities)
            if not any(successes):
                self.logger.log_print(f"error", "Task failed for all entities. Fix and re-run the saved cursor.")
            else:
                self.logger.log_print("info", "Completed one run and reached end of the data set. Done.")
            return(False) # in both cases, it's finished
        else: # all other cases, there's more to do
            successes = self.run_task(entities)
            if not any(successes):
                self.logger.log_print(f"error", "Task failed for all entities. Fix and re-run the saved cursor.")
                return(False)
            else: # save the new cursor and move on
                with open(self.cursor_file, 'wb') as f:
                    pickle.dump(next_cursor, f)
                self.logger.log_print("info", f"Completed one run and the cursor has been saved. Here is the cursor: {next_cursor if next_cursor else '(empty)'}")
                # report end conditions
                return(True)

task = {
"""Pattern for task
To be created for each custom task

Key-Value Pairs:
---------------
name : str,
    name of task that will be used for naming log file
kind : str,
    name of kind that the task addresses
filters : dict,
    will be passed to get_entities convenienve function
limit : int,
    will be passed to get_entities convenienve function
operation : callable lambda or function
    the simple operation to execute on each entity returned by the filters

"""
    "name" : "sample_task",
    "kind" : "kind_name",
    "filters" : {"property_name" : "equality_value"},
    "limit" : 10,
    "operation" : lambda x : print(x) # any function that can operate on a datastore entity
}