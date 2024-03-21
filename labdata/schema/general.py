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
    storage = "ucla_data"     : varchar(12)   # storage name 
    ---
    file_datetime             : datetime      # date created
    file_size                 : double        # using double because int64 does not exist
    file_md5 = NULL           : varchar(32)   # md5 checksum
    '''
    # Files get deleted from AWS if the user has permissions
    def delete(
            self,
            transaction = True,
            safemode  = None,
            force_parts = False):
        
        from ..s3 import s3_delete_file
        from tqdm import tqdm
        filesdict = [f for f in self]
        super().delete(transaction = transaction,
                       safemode = safemode,
                       force_parts = force_parts)
        if len(self) == 0:
            files_not_deleted = []
            storage = filesdict[0]["storage"]
            for s in tqdm(filesdict,desc = f'Deleting objects from s3 {"storage"}:'):
                fname = s["file_path"]
                try:
                    s3_delete_file(fname,
                                   storage = prefs['storage'][s['storage']],
                                   remove_versions = True)
                    
                except Exception as err:
                    print(f'Could not delete {fname}.')
                    files_not_deleted.append(fname)
            if len(files_not_deleted):
                print('\n'.join(files_not_deleted))
                raise(ValueError('''

[Integrity error] Files were deleted from the database but not from AWS.

            Save this message and show it to your database ADMIN.

{0}
                
'''.format('\n'.join(files_not_deleted))))
                    

@dataschema 
class AnalysisFile(dj.Manual):
    definition = '''
    file_path                 : varchar(300)  # Path to the file
    storage = "analysis"      : varchar(12)   # storage name 
    ---
    file_datetime             : datetime      # date created
    file_size                 : double        # using double because int64 does not exist
    file_md5 = NULL           : varchar(32)   # md5 checksum
    '''
    storage = 'analysis'

    # All users with permission to run analysis should also have permission to add and remove files from AWS
    def upload_files(self,src,dataset):
        assert 'subject_name' in dataset.keys(), ValueError('dataset must have subject_name')
        assert 'session_name' in dataset.keys(), ValueError('dataset must have session_name')
        assert 'dataset_name' in dataset.keys(), ValueError('dataset must have dataset_name')
        
        destpath = '{subject_name}/{session_name}/{dataset_name}/'.format(**dataset)
        dst = [destpath+k.name for k in src]
        for d in dst:
            assert len(AnalysisFile() & dict(file_path = d)) == 0, ValueError(
                f'File is already in database, delete it to re-upload {d}.')

        assert self.storage in prefs['storage'].keys(),ValueError(
            'Specify an {self.storage} bucket in preferences["storage"].')
        from ..s3 import copy_to_s3

        copy_to_s3(src, dst, md5_checksum=None, storage_name = self.storage)
        dates = [datetime.utcfromtimestamp(Path(f).stat().st_mtime) for f in src]
        sizes = [Path(f).stat().st_size for f in src]
        md5 = compute_md5s(src)
        # insert in AnalysisFile if all went well
        self.insert([dict(file_path = f,
                          storage = self.storage,
                          file_datetime = d,
                          file_md5 = m,
                          file_size = s) for f,d,s,m in zip(dst,dates,sizes,md5)])
        return [dict(file_path = f,storage = self.storage) for f in dst]

    def delete(
            self,
            transaction = True,
            safemode  = None,
            force_parts = False):
        
        from ..s3 import s3_delete_file
        from tqdm import tqdm
        filesdict = [f for f in self]
        super().delete(transaction = transaction,
                       safemode = safemode,
                       force_parts = force_parts)
        # remove from S3
        if len(self) == 0:
            for s in tqdm(filesdict,desc = 'Deleting objects from s3:'):
                s3_delete_file(s['file_path'],
                               storage = prefs['storage'][s['storage']],
                               remove_versions = True)

        
# This table stores file name and checksums of files that were sent to upload but were processed by upload rules
# There are no actual files associated with these paths
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

#@dataschema
#class BrainArea
        
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
    contents = zip(dataset_type_names)

@dataschema
class Dataset(dj.Manual):
    definition = """
    -> Subject
    -> Session
    dataset_name             : varchar(128)    
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

# Synchronization variables for the dataset live here; these can come from different streams
@dataschema
class DatasetEvents(dj.Imported):
    definition = '''
    -> Dataset
    stream_name                       : varchar(54)   # which clock is used e.g. btss, nidq, bpod
    ---
    stream_time = NULL                 : longblob      # for e.g. the analog channels
    '''
    class Digital(dj.Part):
        definition = '''
        -> master
        event_name                    : varchar(54)
        ---
        event_timestamps = NULL       : longblob  # timestamps of the events
        event_values = NULL           : longblob  # event value or count
        '''
    class AnalogChannel(dj.Part):
        definition = '''
        -> master
        channel_name                 : varchar(54)
        ---
        channel_values = NULL        : longblob  # analog values for channel
        '''

# Upload queue, so that experimental computers are not transfering data 
@dataschema
class UploadJob(dj.Manual):
    definition = '''
    job_id                  : int auto_increment
    ---
    job_waiting = 1         : tinyint             # 1 if the job is up for grabs
    job_status = NULL       : varchar(52)         # status of the job (did it fail?)
    job_host = NULL         : varchar(52)         # where the job is running
    job_rule = NULL         : varchar(52)         # what rule is it following
    job_log = NULL          : varchar(500)        # LOG
    -> [nullable] Dataset                         # optionally insert to dataset
    upload_storage = NULL  : varchar(12)          # storage name, where to upload

    '''
    
    class AssignedFiles(dj.Part):
        definition = '''
        -> master
        src_path               : varchar(300)      # local file path 
        ---
        src_datetime           : datetime          # date created
        src_size               : double            # using double because int64 does not exist
        src_md5 = NULL         : varchar(32)       # md5 checksum
        '''

# Jobs to perform computations, like spike sorting or segmentation
@dataschema
class ComputeTask(dj.Manual):
    definition = '''
    job_id                  : int auto_increment
    ---
    task_waiting = 1         : tinyint             # 1 if the job is up for grabs
    task_name = NULL         : varchar(52)         # what task to run
    task_status = NULL       : varchar(52)         # status of the job (did it fail?)
    task_target = NULL       : varchar(52)         # where to run the job, so it only runs where selected
    task_host = NULL         : varchar(52)         # where the job is running
    task_cmd = NULL          : varchar(2000)       # command to run
    task_parameters = NULL   : varchar(2000)       # command to run after the job finishes
    task_log = NULL          : varchar(2000)       # LOG
    -> [nullable] Dataset                          # dataset
    '''
    
    class AssignedFiles(dj.Part):
        definition = '''
        -> master
        -> File
        '''
    
