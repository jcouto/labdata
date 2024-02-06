from .utils import *
from minio import Minio
# put files to S3
# download files from S3
# move objects to old tier.

def validate_storage(storage):
    '''
    storage = validate_storage(storage)

    Checks that there is an s3 access_key and secret_key in the storage dictionary.
    Updates the labdata preference file.

    Joao Couto - labdata 2024

    '''
    # lets find the storage in the preferences
    if 'storage' in prefs.keys():
        for key in prefs['storage'].keys():
            if prefs['storage'][key] == storage:
                save_prefs = False
                storage = prefs['storage'][key]
    if not 'protocol' in storage.keys():
        # assume S3
        storage['protocol'] = 's3'
        if 'save_prefs' in dir():
            save_prefs = True
    if storage['protocol'] == 's3':
        # then try to find an access key
        for k in ['access_key','secret_key']:
            if not k in storage.keys():
                storage[k] = None
            if storage[k] is None:
                import getpass
                # get the password and write to file
                storage[k] = getpass.getpass(prompt=f'S3 {k}:')
                if 'save_prefs' in dir():
                    save_prefs = True
    if 'save_prefs' in dir():
        if save_prefs:
            save_labdata_preferences(prefs, LABDATA_FILE)                
    return storage


def copyfile_to_s3(source_file,
                   destination_file,
                   storage,
                   md5_checksum = None):
    '''
    Copy a single file to S3 and do a checksum comparisson.

    Joao Couto - 2024
    '''
    
    client = Minio(endpoint = storage['endpoint'],
                   access_key = storage['access_key'],
                   secret_key = storage['secret_key'])

    if 'folder' in storage.keys():
        if len(storage['folder']):
            destination_file = storage['folder'] + '/' + destination_file

    if not md5_checksum is None:
        if not md5_checksum == compute_md5_hash(source_file):
            raise OSError(f'Checksum {md5_checksum} does not match {source_file}.')
    res = client.fput_object(
        storage['bucket'], destination_file, source_file)
    return res

def copy_to_s3(source_files, destination_files,
               storage = None,
               storage_name = None,
               md5_checksum = None,
               n_jobs = 8):
    '''
    Copy S3 and do a checksum comparisson.
    Copy occurs in parallel for multiple files.

    Joao Couto - 2024
    '''
    if storage is None:
        if storage_name is None:
            raise ValueError("Specify a storage to copy to - either pass the storage dictionary or specify a name from the prefs.")
        storage = prefs['storage'][storage_name] # link to preferences storage from storage_name
    storage = validate_storage(storage) # validate and update keys

    if not type(source_files) is list: # check type of source
        raise ValueError('source_files has to be a list of paths')
    
    if not type(destination_files) is list:  # check type of destination
        raise ValueError('destination_files has to be a list of paths')
    # Check if the source and the destination are the correct sizes
    assert len(source_files) == len(destination_files),ValueError('source and destination are the wrong size')
    
    if md5_checksum is None:
        md5_checksum = [None]*len(source_files)
        
    res = Parallel(n_jobs = n_jobs)(delayed(copyfile_to_s3)(src,
                                                            dst,
                                                            storage = storage,
                                                            md5_checksum = md5)
                                    for src,dst,md5 in zip(source_files,destination_files,md5_checksum))
    return res
