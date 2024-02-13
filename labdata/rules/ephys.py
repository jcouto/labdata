from .utils import *

class EphysRule(UploadRule):
    def __init__(self, job_id):
        super(EphysRule,self).__init__(job_id = job_id)
        self.rule_name = 'ephys'

    def _apply_rule(self):
        
        files_to_compress = list(filter(lambda x: '.ap.bin' in x, self.src_paths.src_path.values))
        n_jobs = DEFAULT_N_JOBS
        # compress these in parallel, will work for multiprobe sessions faster?
        res = Parallel(n_jobs = n_jobs)(delayed(compress_ephys_file)(
            filename,
            local_path = self.local_path,
            n_jobs = n_jobs) for filename in files_to_compress)
        
        new_files = np.stack(res).flatten() # stack the resulting files and add them to the path
        self._handle_processed_and_src_paths(files_to_compress, new_files)

    def _post_upload(self):
        if not self.dataset_key is None:
            from ..schema import EphysRecording 
            EphysRecording().add_spikeglx_recording(self.dataset_key)
            
                
############################################################################################################
############################################################################################################

        
def compress_ephys_file(filename, local_path = None, 
                        ext = '.bin',
                        n_jobs = DEFAULT_N_JOBS,
                        check_after_compress = True):
    '''
    Compress ephys data
    '''
    if local_path is None:
        local_path = prefs['local_paths'][0]
    local_path = Path(local_path)
    
    from spks.spikeglx_utils import read_spikeglx_meta
    
    binfile = local_path/filename
    if not binfile.exists():
        raise OSError(f'Could not find binfile to compress ephys {binfile}')
    
    metafile = local_path/str(filename).replace(ext,'.meta')    
    if not metafile.exists():
        raise OSError(f'Could not find metafile to compress ephys {metafile}')

    meta = read_spikeglx_meta(metafile)  # to get the sampling rate and nchannels
    srate = meta['sRateHz']
    nchannels = meta['nSavedChans']
    from mtscomp import compress, decompress
    # Compress a .bin file into a pair .cbin (compressed binary file) and .ch (JSON file).
    cbin,ch = (str(binfile).replace(ext,'.cbin'),str(binfile).replace(ext,'.ch'))
    compress(binfile, cbin, ch,
             sample_rate = srate, n_channels = int(nchannels),
             check_after_compress = check_after_compress,
             chunk_duration = 1, dtype=np.int16, n_threads = n_jobs)
    return cbin.replace(str(local_path),'').strip(pathlib.os.sep),ch.replace(str(local_path),'').strip(pathlib.os.sep)
    

def get_probe_configuration(meta):
    '''
    Meta can be a file or a dictionary.
    Uses spks for now to parse the metadata.
    '''
    if not hasattr(meta,'keys'):
        meta = Path(meta)
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
