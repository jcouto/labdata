from .general import *

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
        if 'probe_id' in metadata: # lets you not be constantly opening the file.
            conf = metadata
        else:
            conf = get_probe_configuration(metadata)
        
        
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
    n_probes               : smallint        # number of probes
    recording_duration     : float           # duration of the recording
    recording_software     : varchar(56)     # software_version
    '''
    
    class ProbeSetting(dj.Part):
        definition = '''
        -> Dataset
        probe_num               : smallint       # probe number
        ---
        -> ProbeConfiguration
        sampling_rate           : float          # sampling rate 

        '''     
    def add_spikeglx_recording(self,key):
        '''
        Adds a recording from Dataset ap.meta files.
        '''
