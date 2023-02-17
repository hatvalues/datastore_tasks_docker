import os
from cursor_utilities import CursorHandler

results = []

task = {
    "name" : "cursor_handler_example_task",
    "kind" : "programme",
    "filters" : {},
    "limit" : 1, # to check first run safely, leave this
    "operation" : lambda entity : results.append(entity.id)
}

chandler = CursorHandler(task)

chandler.logger.clear_log()

chandler.logger.lp_info("STARTING JOB.")
chandler.logger.lp_info(f'GCP Project {os.getenv("DATASTORE_PROJECT_ID")}.')

# first run - to clear the cursor
chandler.logger.lp_info(f'Starting task {task["name"]}')
chandler.run_cursor(first_run=True)
# NOTE: check the results before continuing

# subsequent runs
counter = 0
chandler.task["limit"] = 100
more = True
while more:
    more = chandler.run_cursor(first_run=False) # change the limit
    counter += 1
    chandler.logger.lp_info(f"The loop has run {counter} times")
    print()

chandler.logger.lp_info(f'Number of entities affected: {len(results)}')
chandler.logger.lp_info(f'FINISHED JOB.')



