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
                          parse_filename = True,
                          **kwargs):
    '''
    Copy data between computers; it will not overwrite, unless forced.

    Returns a list of dictionaries with the file paths and md5 checksums.

    Inserts in the Upload table. 

    Joao Couto - labdata 2024
    '''
    
    from .schema import UploadJob,Setup,Subject,Session,Dataset,dj
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

    if any_path_uploaded(filepaths):
        print('Path was already uploaded {0}'.format(Path(filepaths[0]).parent))
        return False
    
    if parse_filename: # parse filename based on the path rules
        tmp = parse_filepath_parts(filepaths[0])
        for k in tmp.keys():
            if not k in kwargs.keys():
                kwargs[k] = tmp[k]
        
    # copy and compute checksum for all paths in parallel.
    res = Parallel(n_jobs = n_jobs)(delayed(_copyfile_to_upload_server)(path,
                                                                        local_path = local_path,
                                                                        server_path = server_path,
                                                                        overwrite = overwrite) for path in filepaths)
    # Add it to the upload table
    # check the job id
    jobid = UploadJob().fetch('job_id')
    if len(jobid):
        jobid = np.max(jobid)
    else:
        jobid = 1
    with dj.conn().transaction:
        print(kwargs)
        if "setup_name" in kwargs.keys():
            Setup.insert1(kwargs, skip_duplicates = True,ignore_extra_fields = True) # try to insert setup
        if "dataset_name" in kwargs.keys() and "session_name" in kwargs.keys() and "subject_name" in kwargs.keys():
            if not len(Subject() & dict(subject_name=kwargs['subject_name'])):
                Subject.insert1(kwargs, skip_duplicates = True,ignore_extra_fields = True) # try to insert subject
                # needs date of birth and gender
            if not len(Session() & dict(subject_name=kwargs['subject_name'],
                                        session_name = 'session_name')):
                Session.insert1(kwargs, skip_duplicates = True,ignore_extra_fields = True) # try to insert session
            if not len(Dataset() & dict(subject_name=kwargs['subject_name'],
                                        session_name = 'session_name',
                                        dataset_name = 'dataset_name')):
                Dataset.insert1(kwargs, skip_duplicates = True,ignore_extra_fields = True) # try to insert dataset
            
        UploadJob.insert1(dict(job_id = jobid, 
                               job_status = "ON SERVER",
                               upload_storage = upload_storage,
                               **kwargs),
                          ignore_extra_fields = True) # Need to insert the dataset first if not there
        
        res = [dict(r, job_id = jobid) for r in res] # add dataset through kwargs
        UploadJob.AssignedFiles.insert(res, ignore_extra_fields=True) # the upload server will run the checksum and upload the files.
    return res

def any_path_uploaded(filepaths):
    '''
    any_path_uploaded(filepaths)

    Checks if any file was already uploaded or on the upload list

    '''
    from .schema import UploadJob, File, ProcessedFile
    # check if the paths are in "Upload or in Files"
    for p in filepaths: # if true for any of the filepaths return True
        if len((File() & f'file_path = "{p}"')) > 0 or len(UploadJob.AssignedFiles() & f'src_path = "{p}"' )>0:
            return True
        if len((ProcessedFile() & f'file_path = "{p}"'))>0:
            return True
    return False # Otherwise return False
            
        
