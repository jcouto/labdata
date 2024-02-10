from .utils import *

class EphysRule(UploadRule):
    def __init__(self, job_id):
        super(EphysRule,self).__init__()
        self.rule_name = 'ephys'

    def apply_rule(self):
        pass



def get_probe_configuration(meta):
    '''
    Meta can be a file or a dictionary.
    Uses spks for now to parse the metadata.
    '''
    if not meta is dict:
        metadata = Path(meta)
        if not meta.exists():
            raise OSError(f'File not found: {meta}')
        try:
            from spks.spikeglx_utils import read_spikeglx_meta
            # TODO: consider porting a minimal version over
        except:
            raise OSError('Could not import spks: install from https://github.com/spkware/spks')
        meta = read_spikeglx_meta(meta)
    
    probe_type = str(int(meta['imDatPrb_type']))
    recording_software = 'spikeglx' # make this work with openephys also
    return dict(probe_id = str(int(meta['imDatPrb_sn'])),
                recording_software = recording_software,
                recording_duration = meta['fileTimeSecs'],
                sampling_rate = meta['sRateHz'],
                probe_type = probe_type,
                probe_n_shanks = 4 if probe_type in ['24','2013','2014'] else 1,
                probe_gain = meta['conversion_factor_microV'][0],
                probe_n_channels = len(meta['channel_idx']),
                channel_idx = meta['channel_idx'],
                channel_coords = meta['coords'],
                channel_shank = meta['channel_shank'],
                probe_recording_channels = int(meta['nSavedChans']-1))
