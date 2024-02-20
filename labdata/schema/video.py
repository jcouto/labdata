from .general import *

@dataschema
class DatasetVideo(dj.Manual):
    definition = '''
    -> Dataset
    video_name           : varchar(56)
    ---
    frame_times = NULL   : longblob
    frame_rate = NULL    : float
    n_frames = NULL      : float
    '''
    class File(dj.Part):
        definition = '''
        -> master
        -> File
        '''

    class Frame(dj.Part):
        definition = '''
        -> master
        frame_num      : int
        ---
        frame          : longblob
        '''

        
