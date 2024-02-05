import os
import sys
import numpy as np
import pandas as pd

import json
import re  # can be removed
from pathlib import Path
from io import StringIO
from glob import glob
from natsort import natsorted

LABDATA_FILE = Path.home()/Path('labdata')/'user_preferences.json'

default_labdata_preferences = dict(cache_paths = [pjoin(os.path.expanduser('~'),'data')],
                                   path_rules='{subject}/{session}/{datatype}',
                                   queues= None,
                                   storage = dict(ucla_data = dict(protocol = 's3',
                                                                   endpoint = 's3.amazonaws.com:9000',
                                                                   bucket = 'churchland-ucla-data',
                                                                   folder = '',
                                                                   access_key = None,
                                                                   secret_key = None)),
                                   database = {
                                       'database.host':'churchland-ucla-data.cxis684q8epg.us-west-1.rds.amazonaws.com',
                                       'database.user': None,
                                       'database.password': None,
                                       'database.labdata_schema': 'lab_data'},
                                   plugins_folder = pjoin(os.path.expanduser('~'),
                                                          'labdata','analysis'),
                                   submit_defaults = None,
                                   run_defaults = {'delete-cache':False},
                                   upload_rules = dict(ephys: dict(
                                       rule: '*.ap.bin',                 # path format that triggers the rule
                                       pre: ['compress_ephys_dataset'],  # functions to execute before
                                       post: ['ingest_ephys_session'],   # function to execute after
                                       use_queue: 'slurm'))) # whether to use a queue and if so which one

def get_labdata_preferences(prefpath = None):
    ''' Reads the user parameters from the home directory.

    pref = get_labdata_preferences(filename)

    filename is a JSON file that is stored in the userfolder/labdata

    Example preference files are in the examples folder.
    '''
    
    if prefpath is None:
        prefpath = LABDATA_FILE
    prefpath = Path(prefpath) # needs to be a file
    preffolder = prefpath.parent
    if not preffolder.exists():
        preffolder.mkdir(parents=True,exist_ok = True)
    if not prefpath.exists():
        with open(prefpath, 'w') as outfile:
            json.dump(default_labdata_preferences, 
                      outfile, 
                      sort_keys = True, 
                      indent = 4)
            print('Saving default preferences to: ' + prefpath)
    with open(prefpath, 'r') as infile:
        pref = json.load(infile)
    for k in default_labdata_preferences:
        if not k in pref.keys():
            pref[k] = default_labdata_preferences[k]
    return pref
