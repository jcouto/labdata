import os
import sys
import numpy as np
import pandas as pd
from datetime import datetime

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
import pickle

# Look into running on g4dn.2xlarge

LABDATA_FILE = Path.home()/Path('labdata')/'user_preferences.json'
DEFAULT_N_JOBS = 8

# dataset_type part of Dataset()
dataset_type_names = ['task-training',
                      'task-behavior',
                      'free-behavior',
                      'imaging-2p',
                      'imaging-widefield',
                      'imaging-miniscope',
                      'ephys',
                      'opto-inactivation',
                      'opto-activation',
                      'analysis']
# The following dictionary describes the equivalency between datatype_name and dataset_type (broadly)
# todo: flip this the other way and split the types with slash or so.
dataset_name_equivalence = dict(ephys ='ephys',
                                task = 'task-training',
                                two_photon = 'imaging-2p',
                                one_photon = 'imaging-widefield',
                                suite2p = 'analysis',
                                kilosort = 'analysis',
                                wfield = 'analysis',
                                deeplabcut = 'analysis',
                                analysis = 'analysis',
                                caiman = 'analysis')
analysis = dict(spks = 'labdata.compute.SpksCompute')

default_labdata_preferences = dict(local_paths = [str(Path.home()/'data')],
                                   scratch_path = str(Path.home()/'scratch'),
                                   path_rules='{subject_name}/{session_name}/{dataset_name}', # to read the session/dataset from a path
                                   queues= None,
                                   allow_s3_download = False,
                                   compute = dict(
                                       aws=dict(access_key = None,
                                                secret_key = None,
                                                region = 'us-west-2',
                                                # check the instructions for how to create the AMI
                                                image_ids = dict(linux = dict(ami = 'ami-0a1aa46f0630cf2c4', 
                                                                              user = 'ubuntu')),
                                                access_key_folder = str(Path.home()/Path('labdata')/'ec2keys')),
                                       containers = dict(
                                           local = str(Path.home()/Path('labdata')/'containers'),
                                           storage = 'analysis'), # place to store on s3
                                       analysis = analysis,
                                       default_target = 'slurm'),
                                   storage = dict(ucla_data = dict(protocol = 's3',
                                                                   endpoint = 's3.amazonaws.com:9000',
                                                                   bucket = 'churchland-ucla-data',
                                                                   folder = '',
                                                                   access_key = None,
                                                                   secret_key = None),
                                                  analysis = dict(protocol = 's3',
                                                                  endpoint = 's3.amazonaws.com:9000',
                                                                  bucket = 'churchland-ucla-analysis',
                                                                  folder = '',
                                                                  access_key = None,
                                                                  secret_key = None)),
                                   database = {
                                       'database.host':'churchland-ucla-data.cxis684q8epg.us-west-1.rds.amazonaws.com',
                                       'database.user': None,
                                       'database.password': None,
                                       'database.name': 'lab_data'},
                                   plugins_folder = str(Path.home()/
                                                        Path('labdata')/'plugins'), # this can be removed?
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
##########################################################

def parse_filepath_parts(path,
                         local_path = None,
                         path_rules=None,
                         session_date_rules = ['%Y%m%d_%H%M%S']):
    
    if path_rules is None:
        path_rules = prefs['path_rules']
    if local_path is None:
        local_path = prefs['local_paths'][0]
    parts = str(path).replace(local_path,'').split(pathlib.os.sep)
    names = [f.strip('}').strip('{') for f in  path_rules.split('/')]
    t = dict()
    for i,n in enumerate(names):
        t[n] = parts[i]
    if 'session_name' in t.keys():
        t['session_datetime'] = datetime.strptime(t['session_name'],session_date_rules[0])
    if 'dataset_name' in t.keys():
        for k in dataset_name_equivalence.keys():
            if k in t['dataset_name'].lower():
                t['dataset_type'] = dataset_name_equivalence[k]
                break # found it..
    return t

##########################################################
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
        res[ipath] = local == check
    return all(res)


def get_filepaths(keys,local_paths = None, download = False):
    '''
    Returns the local path to files and downloads the files if needed. 
    '''

    path = keys.file_path
    pass
    
def find_local_filepath(path,allowed_extensions = [],local_paths = None):
    '''
    Search for a file in local paths and return the path.
    This function exists so that files can be distributed in different file systems.
    List the local paths (i.e. the different filesystems) in labdata/user_preferences.json

    allowed_extensions can be used to find similar extensions 
(e.g. when files are compressed and you want to find the original file)

    localpath = find_local_filepath(path, allowed_extensions = ['.ap.bin'])

    Joao Couto - labdata 2024
    '''
    if local_paths is None:
        local_paths = prefs['local_paths']
        
    for p in local_paths:
        p = Path(p)/path
        if p.exists():
            return p # return when you find the file
        for ex in allowed_extensions:
            p = (p.parent/p.stem).with_suffix(ex)
            if p.exists():
                return p # found allowed extensions (use this to find ProcessedFiles)
            
    return None  # file not found


    
def plugin_lazy_import(name):
    '''
    Lazy import function to load the plugins.
    '''
    import importlib.util
    spec = importlib.util.spec_from_file_location(name, str(Path(prefs['plugins'][name])/"__init__.py"))
    loader = importlib.util.LazyLoader(spec.loader)
    spec.loader = loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    loader.exec_module(module)
    return module


def extrapolate_time_from_clock(master_clock,master_events, slave_events):
    '''
    Extrapolates the time for synchronizing events on different streams
    '''
    from scipy.interpolate import interp1d
    return interp1d(master_events, master_clock, fill_value='extrapolate')(slave_events)


def save_dict_to_h5(filename,dictionary,compression = 'gzip', compression_opts = 1, compression_size_threshold = 1000):
    '''
    Save a dictionary as a compressed hdf5 dataset.
    filename: path to the file (IMPORTANT: this WILL overwrite without checks.)
    dictionary: the dictionary to save

    If the size of the data are larger than compression_size_threshold it will save with compression.
    default compression is gzip, can also use lzf

    Joao Couto - 2023
    '''
    def _save_dataset(f,key,val,
                      compression = compression,
                      compression_size_threshold = compression_size_threshold):
        # compress if big enough.
                
        if np.size(val)>compression_size_threshold:
            extras = dict(compression = compression,
                          chunks = True, 
                          shuffle = True)
            if compression == 'gzip':
                extras['compression_opts'] = compression_opts
        else:
            extras = dict()
        f.create_dataset(str(key),data = val, **extras)

    import h5py
    keys = []
    values = []
    for k in dictionary.keys():
        if not type(dictionary[k]) in [dict]:
            keys.append(k)
            values.append(dictionary[k])
        else:
            for o in dictionary[k].keys():
                keys.append(k+'/'+str(o))
                values.append(dictionary[k][o])
    filename = Path(filename)
    # create file, this will overwrite without asking.
    from tqdm import tqdm
    with h5py.File(filename,'w') as f:
        for k,v in tqdm(zip(keys,values),total = len(keys),desc = f"Saving to hdf5 {filename.name}"):
            _save_dataset(f = f,key = k,val = v) 

def load_dict_from_h5(filename):
    ''' 
    Loads a dictionary from hdf5 file.
    
    This is also in spks.

    Joao Couto - spks 2023

    '''
    data = {}
    import h5py
    with h5py.File(filename,'r') as f:
        for k in f.keys(): #TODO: read also attributes.
            no = k
            if no[0].isdigit():
                no = int(k)
            if hasattr(f[k],'dims'):
                data[no] = f[k][()]
            else:
                data[no] = dict()
                for o in f[k].keys(): # is group
                    ko = o
                    if o[0].isdigit():
                        ko = int(o)
                    data[no][ko] = f[k][o][()]
    return data
