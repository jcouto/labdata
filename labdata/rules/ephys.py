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


def ephys_noise_statistics_from_file(filepath,channel_indices, gain, sampling_rate = 30000, duration = 60):
    '''
    statistics = ephys_noise_statistics_from_file(filepath,channel_indices, gain, sampling_rate = 30000, duration = 60)

    Gets the noise statistics from a raw data file. It won't parse the whole file, instead it will extract 2 chunks, one 
    from t=duration to t=duration*2 and another from t=end of recording-duration*2 to t=end of recording-duration.
    Then computes: the peak to peak, min, max, median and absolute median deviation of those chunks.

    This is useful just to compare the start and end of the recording or to have ballpark estimations of these values. 
    For more accurate measurements split the recording in chunks of e.g. 1 second, compute it for the entire file, then average and std.
    This will max if there are artifacts in the chunks. 

    Joao Couto - labdata 2024
    '''
    
    filepath = Path(filepath)
    if str(filepath).endswith('.cbin'):
        from mtscomp import decompress
        data = decompress(filepath) #,filepath.with_suffix('.ch'))
    elif str(filepath).endswith('.bin'):
        from spks.spikeglx_utils import load_spikeglx_binary
        data,meta = load_spikeglx_binary(filepath)
    else:
        raise ValueError(f'Could not handle extension: {filepath}')
    # read the head and tail data
    head_data = np.array(data[int(sampling_rate*duration):int(sampling_rate*duration)*2],dtype=np.float32)*gain
    tail_data = np.array(data[-int(sampling_rate*duration)*2:-int(sampling_rate*duration)],dtype = np.float32)*gain
    dd = [head_data,tail_data]

    res = dict(channel_peak_to_peak = np.zeros((len(channel_indices),len(dd))),
                 channel_median = np.zeros((len(channel_indices),len(dd))),
                 channel_mad = np.zeros((len(channel_indices),len(dd))),
                 channel_max = np.zeros((len(channel_indices),len(dd))),
                 channel_min = np.zeros((len(channel_indices),len(dd))))
    from scipy.stats import median_abs_deviation
    for i,d in enumerate(dd):
        res['channel_mad'][:,i] = median_abs_deviation(d[:,channel_indices],axis = 0) 
        res['channel_max'][:,i] = np.max(d[:,channel_indices],axis = 0) 
        res['channel_min'][:,i] = np.min(d[:,channel_indices],axis = 0) 
        res['channel_median'][:,i] = np.median(d[:,channel_indices],axis = 0) 
        res['channel_peak_to_peak'][:,i] = res['channel_max'][:,i]-res['channel_min'][:,i]
    return res
