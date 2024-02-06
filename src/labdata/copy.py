from .utils import *

def _copyfile_to_upload_server(filepath, local_path=None, server_path = None,overwrite = False):
    '''
    This is a support function that will copy data between computers; it will not overwrite, unless forced.
    It will raise an exception if the files are already there unless overwrite is true.
    Does not insert to the Upload table.

    Returns a dictionary
    Joao Couto - labdata 2024
    
    '''        
    src = Path(local_path)/filepath
    if src.is_dir():
        raise OSError(f'Can only handle files {src}; copy each file in the folder separately.')
    dst = Path(server_path)/filepath
    if not overwrite and dst.exists():
        raise OSError(f'File {dst} exists; will not overwrite.')
    
    hash = compute_md5_hash(src)  # computes the hash
    srcstat = src.stat()
    file_size = srcstat.st_size
    from shutil import copy2
    dst.parent.mkdir(parents=True, exist_ok = True)
    try:
        copy2(src, dst)
    except:
        raise OSError(f'Could not copy {src} to {dst}.')
    
    return dict(src_path = filepath,
                src_md5 = hash,
                src_size = file_size,
                src_datetime = datetime.fromtimestamp(srcstat.st_ctime))


def copy_to_upload_server(filepaths, local_path = None, server_path = None,
                          upload_storage = None, overwrite = False, n_jobs = 8,
                          **kwargs):
    '''
    Copy data between computers; it will not overwrite, unless forced.

    Returns a list of dictionaries with the file paths and md5 checksums.

    Inserts in the Upload table. 

    Joao Couto - labdata 2024
    '''  
    if local_path is None:  # get the local_path from the preferences
        local_path = prefs['cache_paths'][0]
        if server_path is None:
            raise OSError('Local server path not specified [cache_paths], check the preference file.')
    if server_path is None: # get the upload_path from the preferences
        server_path = prefs['upload_path']
        if server_path is None:
            raise OSError('Upload server path not specified [upload_path], check the preference file.')
    if upload_storage is None: # get the upload_storage name from the preferences
        upload_storage = prefs['upload_storage']
    if not type(filepaths) is list: # Check if the filepaths are in a list
        raise ValueError('Input filepaths must be a list of paths.')
    # replace local_path if the user copied like that by accident.
    filepaths = [str(f).replace(str(local_path),'') for f in filepaths]
    # remove trailing / or \
    filepaths = [f if not f.startswith(pathlib.os.sep) else f[1:] for f in filepaths]
    # copy and compute checksum for all paths in parallel.
    res = Parallel(n_jobs = n_jobs)(delayed(_copyfile_to_upload_server)(
        path,
        local_path = local_path,
        server_path = server_path,
        overwrite = overwrite) for path in filepaths)
    # Add it to the upload table
    from .schema import Upload
    res = [dict(r,
                upload_storage = upload_storage,
                **kwargs) for r in res] # add dataset through kwargs
    Upload.insert(res, ignore_extra_fields=True) # the upload server will now run the checksum and upload the files.
    return res
    
