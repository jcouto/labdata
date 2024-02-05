from .utils import *
from minio import Minio
# put files to S3
# download files from S3
# move objects to old tier.

def validate_storage(storage):
    # lets find the storage in the preferences
    if 'storage' in prefs.keys():
        for key in prefs['storage'].keys():
            if prefs['storage'][key] == storage:
                print(f'Storage is {key}')
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


def copy_to_s3(source_file,destination_file, storage = None, storage_name = None, md5_checksum = None):
    '''
    Copy to S3 and do checksum comparisson.

    Joao Couto - 2024
    '''
    if storage is None:
        if storage_name is None:
            raise OSError("Specify a storage to copy to - either pass the storage dictionary or specify a name from the prefs.")
        storage = prefs['storage'][storage_name]
    storage = validate_storage(storage)
    client = Minio(endpoint = storage['endpoint'],
                   access_key = storage['access_key'],
                   secret_key = storage['secret_key'])

    if 'folder' in storage.keys():
        if len(storage['folder']):
            destination_file = storage['folder'] + '/' + destination_file

    if not md5_checksum is None:
        if not md5_checksum == compute_md5_hash(source_file):
            raise OSError(f'Checksum {md5_checksum} does not match {source_file}.')
    return client.fput_object(
        storage['bucket'], destination_file, source_file)

