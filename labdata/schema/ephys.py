from .general import *
from .procedures import *

@dataschema
class Probe(dj.Manual):
    definition = '''
    probe_id                  : varchar(32)    # probe id to keep track or re-uses
    ---
    probe_type                : varchar(12)    # probe type
    probe_n_shanks            : tinyint        # number of shanks
    probe_recording_channels  : int            # number of recording channels
    '''

@dataschema
class ProbeInsertion(dj.Manual):
    definition = '''
    -> Procedure
    -> Probe
    ---
    insertion_ap         : float   # anterior posterior distance from bregma
    insertion_ml         : float   # median lateral distance from bregma
    insertion_depth      : float   # insertion depth (how much shank is inserted from dura)
    insertion_el         : float   # elevation of the probe (angle)
    insertion_az         : float   # azimuth of the probe  (angle)
    insertion_spin = 0   : float   # spin on the probe shanks
    '''

@dataschema
class ProbeExtraction(dj.Manual):
    definition = '''
    -> Procedure
    -> Probe
    ---
    extraction_successful  : tinyint   # boolean for successfull or not
    '''
    
@dataschema
class ProbeConfiguration(dj.Manual):
    definition = '''
    -> Probe
    configuration_id  : smallint
    ---
    probe_n_channels  : int            # number of connected channels
    probe_gain        : float          # gain of the probe (multiplication factor) 
    channel_idx       : blob           # index of the channels
    channel_shank     : blob           # shank of each channel
    channel_coords    : blob           # channel x,y position
    '''
    def add_from_spikeglx_metadata(self,metadata):
        '''
        Metadata can be a dictionary (with the metadata) or the path to an ap.meta file.
        '''
        from ..rules.ephys import get_probe_configuration
        if not hasattr(metadata,'keys'):
            conf = get_probe_configuration(metadata)
        else:
            conf = metadata
        
        probeid = conf['probe_id']
        if not len(Probe() & f'probe_id = "{probeid}"'):
            Probe.insert1(dict(probe_id = conf['probe_id'],
                               probe_type = conf['probe_type'],
                               probe_n_shanks = conf['probe_n_shanks'],
                               probe_recording_channels = conf['probe_recording_channels']))
        configs = (ProbeConfiguration() & f'probe_id = "{probeid}"').fetch(as_dict = True)
        for c in configs:
            if ((c['channel_coords'] == conf['channel_coords']).all() and
                (c['channel_idx'] == conf['channel_idx']).all()):
                print("The coords are the same, probe is already in there ")
                
                return dict(probe_id = probeid,
                            configuration_id = c['configuration_id'],
                            sampling_rate = conf['sampling_rate'],
                            recording_software = conf['recording_software'],
                            recording_duration = conf['recording_duration'])
        # add to configuration
        confid = len(configs)+1
        ProbeConfiguration.insert1(dict(probe_id = probeid,
                                        configuration_id = confid,
                                        probe_n_channels = conf['probe_n_channels'],
                                        probe_gain = conf['probe_gain'],
                                        channel_idx = conf['channel_idx'],
                                        channel_shank = conf['channel_shank'],
                                        channel_coords = conf['channel_coords']))
        return dict(probe_id = probeid,
                    configuration_id = confid,
                    sampling_rate = conf['sampling_rate'],
                    recording_software = conf['recording_software'],
                    recording_duration = conf['recording_duration'])

@dataschema
class EphysRecording(dj.Imported):
    definition = '''
    -> Dataset
    ---
    n_probes               : smallint            # number of probes
    recording_duration     : float               # duration of the recording
    recording_software     : varchar(56)         # software_version 
    '''
    
    class ProbeSetting(dj.Part):
        definition = '''
        -> master
        probe_num               : smallint       # probe number
        ---
        -> ProbeConfiguration
        sampling_rate           : float          # sampling rate 
        '''
    class ProbeFile(dj.Part):
        definition = '''
        -> EphysRecording.ProbeSetting
        probe_num               : smallint       # probe number
        -> File
        '''
        
    def add_spikeglx_recording(self,key):
        '''
        Adds a recording from Dataset ap.meta files.
        '''
        from ..schema import EphysRecording
        paths = natsorted(list(filter( lambda x: x.endswith('.ap.meta'),
                              pd.DataFrame((Dataset.DataFiles() & key).fetch()).file_path.values)))
        local_path = Path(prefs['local_paths'][0])
        for iprobe, p in enumerate(paths):
            # add each configuration
            tmp = ProbeConfiguration().add_from_spikeglx_metadata(local_path/p)
            tt = dict(key,n_probes = len(paths),probe_num = iprobe,**tmp)
            EphysRecording.insert1(tt,
                                   ignore_extra_fields = True,
                                   skip_duplicates = True,
                                   allow_direct_insert = True)
            EphysRecording.ProbeSetting.insert1(tt,
                                                ignore_extra_fields = True,
                                                skip_duplicates = True,
                                                allow_direct_insert = True)
            # only working for spikeglx files for the moment.
            pfiles = list(filter(lambda x: f'imec{iprobe}.ap.' in x,paths)) 
            EphysRecording.ProbeFile().insert([
                dict(tt,
                     **(File() & f'file_path = "{fi}"').proj().fetch(as_dict = True)[0])
                for fi in pfiles],
                                              skip_duplicates = True,
                                              ignore_extra_fields = True,
                                              allow_direct_insert = True)
            EphysRecordingNoiseStats().populate(tt) # try to populate the NoiseStats table (this will take a couple of minutes)

@dataschema
class EphysRecordingNoiseStats(dj.Computed):
    # Statistics to access recording noise on multisite 
    definition = '''
    -> EphysRecording.ProbeSetting
    ---
    channel_median = NULL             : longblob  # nchannels*2 array, the 1st column is the start, 2nd at the end of the file
    channel_max = NULL                : longblob 
    channel_min = NULL                : longblob
    channel_peak_to_peak = NULL       : longblob
    channel_mad = NULL                : longblob  # median absolute deviation
    '''
    duration = 30                     # duration of the stretch to sample (takes it from the start and the end of the file)

    def make(self,key):
        files = pd.DataFrame((EphysRecording.ProbeFile() & key).fetch())
        assert len(files), ValueError(f'No files for dataset {key}')
        # search for the recording files (this is set for compressed files now)
        recording_file = list(filter(lambda x : 'ap.cbin' in x,files.file_path.values))
        assert len(recording_file),ValueError(f'Could not find ap.cbin for {key}. Check Dataset.DataFiles?')
        recording_file = recording_file[0]
        filepath = find_local_filepath(recording_file, allowed_extensions = ['.ap.bin'])
        assert not filepath is None, ValueError(f'File [{recording_file}] not found in local paths.')
        # to get the gain, the channel_indices, and the sampling rate
        config = pd.DataFrame((ProbeConfiguration()*EphysRecording.ProbeSetting() & key).fetch()).iloc[0]
        # compute
        from ..rules.ephys import ephys_noise_statistics_from_file
        noisestats = ephys_noise_statistics_from_file(filepath,
                                                      duration = self.duration,
                                                      channel_indices = config.channel_idx,
                                                      sampling_rate = config.sampling_rate,
                                                      gain = config.probe_gain)        
        self.insert1(dict(key,**noisestats),ignore_extra_fields = True)

    
@dataschema
class EphysAnalysisParams(dj.Manual):
    definition = '''
    parameter_set_num      : int            # number of the parameters set
    ---
    algorithm_name         : varchar(64)    # preprocessing  and spike sorting algorithm 
    parameters_dict        : varchar(2000)  # parameters json formatted dictionary
    code_link = NULL       : varchar(300)   # the software that preprocesses and sorts
    '''

@dataschema
class SpikeSorting(dj.Manual):
    definition = '''
    -> EphysRecording.ProbeSetting
    -> EphysAnalysisParams
    ---
    n_pre_samples                     : smallint   # to compute the waveform time 
    n_sorted_units    = NULL          : int        # number of sorted units
    n_detected_spikes = NULL          : int        # number of detected spikes
    sorting_datetime = NULL           : datetime   # date of the spike sorting analysis
    channel_indices = NULL            : longblob   # channel_map
    channel_coords = NULL             : longblob   # channel_positions
   '''
    
    class Segment(dj.Part):
        definition = '''
        -> master
        segment_num                   : int  # number of the segment
        ---
        offset_samples                : int         # offset where the traces comes from
        segment                       : longblob    # 2 second segment of data in the AP band
        '''
        
    class Unit(dj.Part):
        definition = '''
        -> master
        unit_id                  : int       # cluster id
        ---
        spike_times              : longblob  # in samples (uint64)
        spike_positions = NULL   : longblob  # spike position in the electrode
        '''
        
    class Features(dj.Part):
        definition = '''
        -> SpikeSorting.Unit
        ---
        amplitudes = NULL  : longblob        # template amplitudes for each unit
        pc_features = NULL : longblob        # Principal Component features
        '''

    class Templates(dj.Part):
       definition = '''
       -> SpikeSorting
       ---
       pc_features_idx = NULL   : longblob    # template index for each pc feature
       templates = NULL          : longblob    # templates
       whitening_matrix = NULL  : longblob    # whitening_matrix_inv.npy
       '''
       
    class Waveforms(dj.Part):
        definition = '''
        -> SpikeSorting.Unit
        ---
        waveform_median   :  longblob         # average waveform
        -> AnalysisFile
        '''

@dataschema
class UnitMetrics(dj.Computed):
   # Compute the metrics from the each unit,
   # so we can recompute and add new ones if needed and not depend on the clustering
   definition = '''
   -> SpikeSorting.Unit
   ---
   n_spikes                 : int
   isi_violations = NULL    : float
   amplitude_cutoff = NULL  : float
   '''

#   #
