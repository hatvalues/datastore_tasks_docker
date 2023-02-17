import os
from utilities import get_entities, StandardLogger, datastore_client

# prevent multiple instances of the log file handler
if not "lg" in globals():
    lg = StandardLogger("simple_example_task")

lg.clear_log()

lg.lp_info("STARTING JOB.")

kind = "school"
filters = {"country" : "Wales"}
schools = get_entities(kind=kind, filters=filters)
lg.lp_info(f'There are {len(schools)} entities returned from kind {kind} with filters {filters} in GCP Project {os.getenv("DATASTORE_PROJECT_ID")}')

# do the work here
update_kind = "organisation"
for s in schools:
    lg.lp_info(f'Running task on {kind} entity {s.id}')
    # for example, you could get a related entity and perform an update
    key = datastore_client.key(update_kind, s["organisation"])
    org = datastore_client.get(key)
    if org:
        lg.lp_info(f'Related {update_kind} entity was last modified by {org["lastModifiedBy"]}')
    else:
        lg.lp_error(f'There is no related {update_kind} for {kind} entity {s.id}')
    

lg.lp_info("Verifying job.")
# do some useful check here e.g. how many entities have a new value?
schools = get_entities(kind=kind, filters=filters)
lg.lp_info(f'After running job, there are {len(schools)} entities returned from kind {kind} with filters {filters} in GCP Project {os.getenv("DATASTORE_PROJECT_ID")}')
lg.lp_info("FINISHED JOB.")