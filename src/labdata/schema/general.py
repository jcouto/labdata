from ..utils import *
import datajoint as dj

if 'database' in prefs.keys():
    for key in prefs['database'].keys():
        if prefs['database'][key] is None:
            if key in ['database.user', 'database.password']:
                import getpass
                # get the password and write to file
                prefs['database'][key] = getpass.getpass(prompt=key)
                gotpass = True
        if not prefs['database'][key] is None:
            dj.config[key] = prefs['database'][key]
    if 'gotpass' in dir():
        # overwrite the preference file (save the datajoint password.)
        save_labdata_preferences(prefs, LABDATA_FILE)                

dataschema = dj.schema(dj.config['database.name'])

@dataschema 
class File(dj.Manual):
    definition = '''
    file_path                 : varchar(300)  # Path to the file
    storage                   : varchar(12)   # storage name 
    ---
    file_datetime             : datetime      # date created
    file_size                 : double        # using double because int64 does not exist
    file_md5 = NULL           : varchar(32)   # md5 checksum
    '''
# this table stores file name and checksums of files that were sent to upload but were processed by upload rules
@dataschema
class ProcessedFile(dj.Manual): 
    definition = '''
    file_path                 : varchar(300)  # Path to the file that was processe (these are not in S3)
    ---
    file_datetime             : datetime      # date created
    file_size                 : double        # using double because int64 does not exist
    file_md5 = NULL           : varchar(32)   # md5 checksum
    '''
        
@dataschema
class LabMember(dj.Manual):
    definition = """
    user_name                 : varchar(32)	# username
    ---
    email=null                : varchar(128)	# email address
    first_name = null         : varchar(32)	# first name
    last_name = null          : varchar(32)   	# last name
    date_joined               : date            # when the user joined the lab
    is_active = 1             : boolean	        # active or left the lab
    """

@dataschema
class Species(dj.Lookup):
    definition = """
    species_name              : varchar(32)       # species nickname
    ---
    species_scientific_name   : varchar(56)	  # scientific name 
    species_description=null  : varchar(256)       # description
    """
    
@dataschema
class Strain(dj.Lookup):
    definition = """
    strain_name                : varchar(56)	# strain name
    ---
    -> Species
    strain_description=null    : varchar(256)	# description
    """

@dataschema
class Subject(dj.Manual):
    ''' Experimental subject.'''
    definition = """
    subject_name               : varchar(20)          # unique mouse id
    ---
    subject_dob                : date                 # mouse date of birth
    subject_gender             : enum('M', 'F', 'U')  # sex of mouse - Male, Female, or Unknown
    -> Strain
    -> LabMember
    """

@dataschema
class SetupLocation(dj.Lookup):
    definition = """
    setup_location    : varchar(255)   # room 
    ---
"""
    contents = zip(['CHS-74100'])

@dataschema
class Setup(dj.Lookup):
    definition = """
    setup_name        : varchar(54)     # setup name          
    ---
    -> SetupLocation 
    setup_description : varchar(512) 
"""

@dataschema
class Note(dj.Manual):
    definition = """
    -> LabMember.proj(notetaker='user_name')
    note_datetime       : datetime
    ---
    notes = ''          : varchar(4000)   # free-text notes
    """
    class Image(dj.Part):
        definition = """
        -> Note
        image_id       : int
        ---
        image          : longblob
        caption = NULL : varchar(256)
        """
    class Attachment(dj.Part):
        definition = """
        -> Note
        -> File
        ---
        caption = NULL : varchar(256)
        """
    
@dataschema
class Session(dj.Manual):
    definition = """
    -> Subject
    session_name             : varchar(54)     # session identifier
    ---
    session_datetime         : datetime        # experiment date
    -> [nullable] LabMember.proj(experimenter = 'user_name') 
    """

@dataschema
class DatasetType(dj.Lookup):
    definition = """
    dataset_type: varchar(32)
    """
    contents = zip(['task-training',
                    'task-behavior',
                    'free-behavior',
                    'imaging-2p',
                    'imaging-widefield',
                    'imaging-miniscope',
                    'ephys',
                    'opto-inactivation',
                    'opto-activation',
                    'analysis'])

@dataschema
class Dataset(dj.Manual):
    definition = """
    -> Subject
    -> Session
    dataset_name             : varchar(32)    
    ---
    -> [nullable] DatasetType
    -> [nullable] Setup
    -> [nullable] Note
    """
    class DataFiles(dj.Part):  # the files that were acquired on that dataset.
        definition = '''
        -> master
        -> File
        '''        

@dataschema 
class Upload(dj.Manual):  # add stuff to the upload table that the computer should upload. 
    definition = '''
    src_path               : varchar(300)      # local file path 
    ---
    src_datetime           : datetime          # date created
    src_size               : double            # using double because int64 does not exist
    src_md5 = NULL         : varchar(32)       # md5 checksum
    upload_storage = NULL  : varchar(12)       # storage name, where to upload
    -> [nullable] Dataset                      # optionally insert to dataset
    '''

    def put(self, key=None, local_path = None, storage_name = None):  # this actually does the upload and checks
        '''
        Upload data to S3.
        '''
        if key is None:
            keys = pd.DataFrame(self.fetch())
        else:
            keys = pd.DataFrame((self & key).fetch())
        # using only one local path; to do: iterate
        if local_path is None:
            assert 'local_paths' in prefs.keys(), ValueError('Preferences need to have "local_paths"')
            assert type(prefs['local_paths']) is list, ValueError('"local_paths" has to be a list; check preference examples.')
            local_path = prefs['local_paths'][0]

        # get the storage to upload
        if storage_name is None:         
            if 'upload_storage' in prefs.keys():
                storage_name = prefs['upload_storage']

        # split the files by folder;
        paths = [Path(local_path) / p for p in keys.src_path.values]
        # get only the paths that exist
        idx = np.where([p.exists() for p in paths])[0]
        keys = keys.iloc[idx]
        paths = [paths[i] for i in idx]
        # get the folder names
        keys['foldername'] = [p.parent for p in paths]
        # do one folder at a time
        for folder in np.unique(keys['foldername']):
            nk = keys[keys.foldername == folder]
            # need to check if there are any rules to apply to the dataset.
            
            # if so, then one needs to work at the folder level.
            # 1) do the checksum, if any is wrong, don't do it.
            # 2) process the folder
            # 3) get the new filenames and upload
            
            # destination in the bucket is actually the path
            dest = [k for k in nk.src_path.values]
            # source is the place where data are
            src = [Path(local_path) / p for p in nk.src_path.values]
            # hashes are computed
            hashes = [p for p in nk.src_md5.values]
            # s3 copy in parallel
            copy_to_s3(src,dest,md5_checksum=hashes,storage_name=storage_name)
            # remove from the Upload table
            with dj.conn().transaction:
                # insert to File
                # insert to Dataset.DataFiles

                # delete from Upload
                [(Upload() & 'src_path = "{0}"'.format(d.src_path)).delete(safemode = False) for i,d in nk.iterrows()]
        return 

@dataschema
class UploadJob(dj.Manual):
    definition = '''
    job_id                  : int auto_increment
    ---
    job_waiting = 1         : tinyint             # if the job is up for grabs
    job_status = NULL       : varchar(52)         # status of the job (did it fail?)
    job_host = NULL         : varchar(52)         # where the job is running
    job_log = NULL          : varchar(500)        # LOG
    '''
    class AssignedFiles(dj.Part):
        definition = '''
        -> master
        -> Upload
        '''
