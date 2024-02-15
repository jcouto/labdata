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

# Synchronization variables for the dataset live here; these can come from different streams
@dataschema
class DatasetEvents(dj.Imported):
    definition = '''
    -> Dataset
    stream_name                       : varchar(54)   # which clock is used e.g. btss, nidq, bpod
    ---
    analog_times = NULL               : longblob      # for e.g. the analog events
    '''
    class DigitalEvent(dj.Part):
        definition = '''
        -> master
        event_name                    : varchar(54)
        ---
        event_onsets = NULL           : longblob  # timestamps of the events
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
    job_waiting = 1         : tinyint             # 1 if the job is up for grabs
    job_status = NULL       : varchar(52)         # status of the job (did it fail?)
    job_target = NULL       : varchar(52)         # where to run the job, so it only runs where selected
    job_host = NULL         : varchar(52)         # where the job is running
    job_cmd = NULL          : varchar(500)        # command to run
    job_log = NULL          : varchar(500)        # LOG
    -> [nullable] Dataset                         # dataset
    job_post_cmd = NULL     : varchar(500)        # command to run after the job finishes

    '''
    
    class AssignedFiles(dj.Part):
        definition = '''
        -> master
        -> File
        '''
    
