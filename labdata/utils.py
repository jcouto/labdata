import os
import sys
import numpy as np
import pandas as pd

import json
import re  # can be removed?
from pathlib import Path
from io import StringIO
from glob import glob
from natsort import natsorted
import hashlib
from datetime import datetime
import pathlib
from joblib import delayed, Parallel

# Look into running on g4dn.2xlarge

LABDATA_FILE = Path.home()/Path('labdata')/'user_preferences.json'
DEFAULT_N_JOBS = 8
default_labdata_preferences = dict(local_paths = [str(Path.home()/'data')],
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
                                       'database.name': 'lab_data'},
                                   plugins_folder = str(Path.home()/
                                                        Path('labdata')/'plugins'),
                                   submit_defaults = None,
                                   run_defaults = {'delete-cache':False},
                                   upload_path = None,           # this is the path to the local computer that writes to s3
                                   upload_storage = None,        # which storage to upload to
                                   upload_rules = dict(ephys = dict(
                                       rule = '*.ap.bin',                 # path format that triggers the rule
                                       pre = ['compress_ephys_dataset'],  # functions to execute before
                                       post = ['ingest_ephys_session'],   # function to execute after
                                       use_queue = 'slurm')))             # whether to use a queue and if so which one

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
        save_labdata_preferences(default_labdata_preferences, prefpath)
    with open(prefpath, 'r') as infile:
        pref = json.load(infile)
    for k in default_labdata_preferences:
        if not k in pref.keys():
            pref[k] = default_labdata_preferences[k]
    from socket import gethostname
    pref['hostname'] = gethostname()
    return pref

def save_labdata_preferences(preferences, prefpath):
    with open(prefpath, 'w') as outfile:
        json.dump(preferences, 
                  outfile, 
                  sort_keys = True, 
                  indent = 4)    
        print(f'Saving default preferences to: {prefpath}')

prefs = get_labdata_preferences()


##########################################################
        
def compute_md5_hash(fname):
    '''
    Computes the md5 hash that can be used to check file integrity
    '''
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def compute_md5s(filepaths,n_jobs = DEFAULT_N_JOBS):
    '''
    Computes the checksums for multiple files in parallel 
    '''
    return Parallel(n_jobs = n_jobs)(delayed(compute_md5_hash)(filepath) for filepath in filepaths)


def compare_md5s(paths,checksums, n_jobs = DEFAULT_N_JOBS):
    '''
    Computes the checksums for multiple files in parallel 
    '''
    localchecksums = compute_md5s(paths, n_jobs = n_jobs)
    res = [False]*len(paths)
    assert len(paths) == len(checksums), ValueError('Checksums not the same size as paths.')
    for ipath,(local,check) in enumerate(zip(localchecksums,checksums)):
        res[ipath] = locall == check
    return all(res)
