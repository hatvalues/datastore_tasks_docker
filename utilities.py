"""Common Utilities TODO: Make this into a distributable library?

Convenience functions for getting and creating data store entities
Some other common hacks
"""
import logging
import os
from datetime import datetime
import hashlib
from itertools import chain

GCP_MAX_KEYS = 500 # maximum number of keys to write at one time. Hard limit from GCP.

# convenience functions for datastore access
from google.cloud import datastore, storage, bigquery
datastore_client = datastore.Client()
storage_client = storage.Client()
bigquery_client = bigquery.Client()

def query_from_(kind, filters, projection):
    """Internal function. Doesn't make sense to use directly.

    Parameters
    ----------
    kind : str
        name of datastore kind to address
    filters : dict, keys are str, values are str or num
        dictionary will be upacked into equality filters
        for any other kind of filter, better hand-code the query
    projection : list of str
        property names to collect from datastore
        only works if composite index is set up to meet projection

    Returns
    -------
    datastore.Client.query object
    """
    if filters:
        filters_list = [(key,"=",val) for key,val in filters.items()]
        query = datastore_client.query(kind=kind,filters=filters_list)
    else:
        query = datastore_client.query(kind=kind)
    if projection:
        # NOTE: can't filter and project the same property
        if isinstance(projection, list):
            projection = [p for p in projection if p not in filters.keys()]
            query.projection = projection
        elif isinstance(projection, str) and projection not in filters.keys():
            query.projection = [projection]
    return query

def get_entities(kind, filters={}, projection=[], limit=None):
    """Convenience function to get entities from a datastore kind.

    Parameters
    ----------
    kind : str
        name of datastore kind to address
    filters : (optional) dict, keys are str, values are str or num
        dictionary will be upacked into equality filters
        for any other kind of filter, better hand-code the query
    projection : (optional) list of str
        property names to collect from datastore
        only works if composite index is set up to meet projection
    limit : (optional) int
        number of entities to fetch

    Returns
    -------
    list of datastore.Entity
    """
    query = query_from_(kind, filters, projection)
    return(list(query.fetch(limit=limit)))

def get_entity_keys(kind, filters={}, limit=None):
    """Convenience function to get keys only entities from a datastore kind.

    Parameters
    ----------
    kind : str
        name of datastore kind to address
    filters : (optional) dict, keys are str, values are str or num
        dictionary will be upacked into equality filters
        for any other kind of filter, better hand-code the query
    limit : (optional) int
        number of entities to fetch

    Returns
    -------
    list of datastore.Entity
    """
    query = query_from_(kind, filters, projection=[])
    query.keys_only()
    return(list(query.fetch(limit=limit)))

def create_entity(kind, properties, dedup_check, excludes=[], parents=[], convenience_id=False):
    """Convenience function to create entities from dictionaries to datastore kind.

    Parameters
    ----------
    kind : str
        name of datastore kind to address
    properties : dict, keys are str, values are anything
        keys are property names, values are the property values
    dedup_check : dict, keys are str, values are str or num
        dictionary will be upacked into equality filters
        must contain something to avoid scanning the whole kind, then do nothing
        so e.g. check for something that already has a unique identifier
    excludes : (optional) list of str
        property names to exclude from indexes - use for long text
    parents : (optional) dict, keys are str, values are usually long integers used by GCP for key ids
        number of entities to fetch
    convenience_id : (optional) Boolean
        when true, a property name "id", value entity.key.id, will be added to the entity

    Returns
    -------
    datastore.Key
    """
    if not isinstance(properties, dict) and not isinstance(dedup_check, dict):
        raise(ValueError("properties and dedup_check must be a dict."))
    if not dedup_check:
        # dedup_check works like get_entity filters so do not provide an empty dict or else it will scan the whole kind and then do nothing, waste of time and resources
        return None
    entities = get_entities(kind=kind, filters=dedup_check)
    if entities:
        return None
    else:
        # parents must be a list of dict like [{"grandparent_kind" : <int>}, {"parent_kind" : <int>}]
        if parents:
            for p in parents:
                if not isinstance(list(p.keys())[0], str) or not isinstance(list(p.values())[0], int):
                    raise(ValueError('parents must be a list like [{"grandparent_kind" : <int>}, {"parent_kind" : <int>}]'))
        # the following pulls out the parent kind and key, the finally adds the target kind to a generator. The * expands this out into individual arguments
        partial_key = datastore_client.key(*(c for c in chain.from_iterable([[list(p.keys())[0], list(p.values())[0]] for p in parents] + [[kind]])))
        keys = datastore_client.allocate_ids(partial_key, 1)
        key = keys[0]
        entity = datastore.Entity(key)
        # exclude any non-indexing properties (especially long text)
        if excludes:
            if isinstance(excludes, str):
                excludes = [excludes]
            for exclude in excludes:
                entity.exclude_from_indexes.add(exclude)
        # some entities need a property id
        if convenience_id:
            properties.update({"id" : key.id})
        # finally put the entity to the data store    
        entity.update(properties)
        datastore_client.put(entity)
    return key

def create_nesting_entity(properties, excludes=[]):
    """Convenience function to create anonymous entities from dictionaries.
    Can be used to update existing entities e.g. for nesting values with exclude from indexes.

    Parameters
    ----------
    properties : dict, keys are str, values are anything
        keys are property names, values are the property values
    excludes : (optional) list of str
        property names to exclude from indexes - use for long text
    
    Returns
    -------
    datastore.Entity
    """
    entity = datastore.Entity(exclude_from_indexes=excludes)
    entity.update(properties)
    return(entity)

class StandardLogger():
    """
    Class to call for standardising logging output.
    Convenience functions will log to file and print to screen.

    lp_<logging leverl>(msg, to_screen=True)
        log msg with the named logging level. Print to screen or not.

    clear_log()
        Deletes the content of the log file of same name used to instantiate the instance
    """
    def __init__(self, process_name):
        # standardise all logging of processes
        self.log_dir = os.getenv("LOG_LOCATION") or "logs"
        if not os.path.isdir(self.log_dir):
            os.mkdir(self.log_dir)
        self.log_file = f'{self.log_dir}/{process_name}.log'
        self.logger = logging.getLogger(process_name)
        self.logger.setLevel(logging.INFO)
        self.log_handler = logging.FileHandler(self.log_file)
        self.log_handler.setLevel(logging.INFO)
        self.log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.log_handler.setFormatter(self.log_formatter)
        self.logger.addHandler(self.log_handler)
    # BUG: When this class is called in another module
    # it must be actively deleted to kill the file reference.
    # Also, actively check that the logger is not in globals
    # If not, lines will be duplicated on the log file as a second (third, fourth) reference is opened
    def __del__(self):
        self.logger.removeHandler(self.log_handler)
        self.log_handler.close()
        del self.log_handler
    def clear_log(self):
        if os.path.exists(self.log_file):
            with open(self.log_file, "w") as lf:
                lf.write("")
    # you can log only the usual way, because this class returns a logger
    # the following methods allow logging as usual and simultaneoulsy printing to screen for interactive monitoring
    def log_print(self, level, msg, to_screen):
        level_switch = {"debug" : self.logger.debug,
                        "info" : self.logger.info,
                        "warning" : self.logger.warning,
                        "error" : self.logger.error
                        }
        level_switch[level](msg)
        if to_screen:
            print(f"{level}: {datetime.utcnow().isoformat()} {msg}")
    def lp_debug(self, msg, to_screen=True):
        self.log_print("debug", msg, to_screen)
    def lp_info(self, msg, to_screen=True):
        self.log_print("info", msg, to_screen)
    def lp_warning(self, msg, to_screen=True):
        self.log_print("warning", msg, to_screen)
    def lp_error(self, msg, to_screen=True):
        self.log_print("error", msg, to_screen)

# other convenience function
# replace a Falsity (None, [], (), {}, "") with an alternative value
def val_or_alt(val, alt):
    """Returns given falsity if value was any kind of falsity.
    
    Parameters
    ----------
    val : anything
        If this has a value, return it. If it is a falsity, retern the requested falsity type..
    alt : anything
        works best with falsities that it's otherwise inconvenient to test with "or"

    Returns
    -------
    val if has value, else alt
    """
    if val:
        return(val)
    else:
        return(alt)

def md5(string):
    """MD5 Hash of anything

    Parameters
    ----------
    string : str

    Returns
    -------
    str
    """
    return(hashlib.md5(string.encode('UTF-8')).hexdigest())

# chunk a list - for put_multi, get_multi etc
def chunks_from_(input_list, chunk_size = GCP_MAX_KEYS):
    """Split a list into list of lists. Convenient for working in bulk with the datastore.
    """
    return [input_list[i:i + chunk_size] for i in range(0, len(input_list), chunk_size)]

# chunk a list - generator version
def gen_chunks_from_(input_list, chunk_size = GCP_MAX_KEYS):
    """Split a list into generator of lists. Convenient for working in bulk with the datastore.
    """
    for i in range(0, len(input_list), chunk_size):
        yield input_list[i:i + chunk_size]