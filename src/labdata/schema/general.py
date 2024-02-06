from ..utils import *
import datajoint as dj

#with dj.conn().transaction

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
    file_path :            varchar(300)  # Path to the file
    storage:               varchar(12)   # storage name 
    ---
    file_datetime:         datetime      # date created
    file_size:             double        # using double because int64 does not exist
    file_md5 = NULL:       varchar(32)   # md5 checksum
    '''
    
@dataschema 
class Upload(dj.Manual):  # add stuff to the upload table that the computer should upload. this should
    definition = '''
    src_path :            varchar(300)  # local file path 
    upload_storage:       varchar(12)   # storage name
    upload_host:          varchar(24)   # name of the host computer that should upload
    ---
    src_datetime:         datetime      # date created
    src_size:             double        # using double because int64 does not exist
    checksum_md5 = NULL:  varchar(32)   # md5 checksum
    '''
    
@dataschema
class LabMember(dj.Manual):
    definition = """
    user_name:		                varchar(32)	# username
    ---
    email=null:		                varchar(128)	# email address
    first_name=null:                    varchar(32)	# first name
    last_name=null:	                varchar(32)   	# last name
    date_joined:	                date            # when the user joined the lab
    is_active = 1:                      boolean	        # active or left the lab
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
    strain_ts=CURRENT_TIMESTAMP: timestamp
    """

@dataschema
class Subject(dj.Manual):
    ''' Experimental subject.'''
    definition = """
    subject_name  : varchar(20)          # unique mouse id
    ---
    subject_dob       : date                 # mouse date of birth
    subject_gender    : enum('M', 'F', 'U')  # sex of mouse - Male, Female, or Unknown
    -> Strain
    -> LabMember
    """

@dataschema
class SetupLocation(dj.Lookup):
    definition = """
    setup_location    : varchar(255) 
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
    -> LabMember
    note_datetime       : datetime
    ---
    notes = ''          : varchar(4000)   # free-text notes
    """
    class Image(dj.Part):
        definition = """
        -> Note
        image_id       : int
        ---
        image          :  longblob
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
    session                  : varchar(54)     # session identifier
    ---
    session_datetime         : datetime        # experiment date
    -> [nullable] LabMember 
    -> [nullable] Setup
    -> [nullable] Note
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
                    'analysis',
                    'unknown'])

@dataschema
class Dataset(dj.Manual):
    definition = """
    -> Subject
    -> Session
    -> DatasetType
    ---
    -> [nullable] LabMember 
    -> [nullable] Setup
    -> [nullable] Note
    """
    class DataFiles(dj.Part):  # the files that were acquired on that dataset.
        definition = '''
        -> master
        -> File
        '''        

