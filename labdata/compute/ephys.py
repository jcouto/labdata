from ..utils import *
from .utils import BaseCompute

class SpksCompute(BaseCompute):
    container = 'labdata_spks'
    name = 'spks'
    url = 'http://github.com/spkware/spks'
    def __init__(self,job_id, allow_s3 = None,**kwargs):
        super(SpksCompute,self).__init__(job_id, allow_s3 = None)
        self.file_filters = ['.ap.']
        # default parameters
        self.parameters = dict(algorithm_name = 'spks_kilosort2.5',
                               motion_correction = True,
                               low_pass = 300.,
                               high_pass = 13000.,
                               thresholds = [9.,3.])
        self.parameter_set_num = None # identifier in SpikeSortingParams
        self._init_job()
        if not self.job_id is None:
            self.add_parameter_key()

    def add_parameter_key(self):
        parameter_set_num = None
        from ..schema import SpikeSorting, SpikeSortingParams, EphysRecording, DatasetEvents
        # check if in spike sorting
        parameters = pd.DataFrame(SpikeSortingParams().fetch())
        for i,r in parameters.iterrows():
            # go through every algo
            if self.parameters == json.loads(r.parameters_dict):
                parameter_set_num = r.parameter_set_num
        if parameter_set_num is None:
            if len(parameters) == 0:
                parameter_set_num = 1
            else:
                parameter_set_num = np.max(parameters.parameter_set_num.values)+1

        if not parameter_set_num in parameters.parameter_set_num.values:
            SpikeSortingParams().insert1(dict(parameter_set_num = parameter_set_num,
                                               algorithm_name = self.parameters['algorithm_name'],
                                               parameters_dict = json.dumps(self.parameters),
                                               code_link = self.url),
                                          skip_duplicates=True)
        self.parameter_set_num = parameter_set_num
        recordings = EphysRecording.ProbeSetting() & dict(self.dataset_key)
        sortings = SpikeSorting() & dict(self.dataset_key)
        if len(recordings) == len(sortings):
            self.set_job_status(
                job_status = 'FAILED',
                job_waiting = 0,
                job_log = f'{self.dataset_key} was already sorted with parameters {self.parameter_set_num}.')    
            raise(ValueError(f'{self.dataset_key} was already sorted with parameters {self.parameter_set_num}.'))
           
    def _secondary_parse(self,arguments):
        '''
        Handles parsing the command line interface
        '''
        import argparse
        parser = argparse.ArgumentParser(
            description = 'Analysis of spike data using kilosort version 2.5 through the spks package.',
            usage = 'spks -a <SUBJECT> -s <SESSION> -- <PARAMETERS>')
        
        parser.add_argument('-p','--probe',
                            action='store', default=None, type = int,
                            help = "THIS DOES NOTHING NOW. WILL BE FOR OPENING PHY")
        parser.add_argument('-l','--low-pass',
                            action='store', default=self.parameters['low_pass'], type = float,
                            help = "Lowpass filter (default 300.Hz)")
        parser.add_argument('-i','--high-pass',
                            action='store', default=self.parameters['high_pass'], type = float,
                            help = "Highpass filter (default 13000.Hz)")
        parser.add_argument('-t','--thresholds',
                            action='store', default=self.parameters['thresholds'], type = float,
                            help = "Thresholds for spike detection default [9.,3.]")

        parser.add_argument('-n','--no-motion-correction',
                            action='store_false', default = self.parameters['motion_correction'],
                            help = "Skip motion correction")

        args = parser.parse_args(arguments[1:])


        self.parameters['motion_correction'] = args.no_motion_correction
        self.parameters['low_pass'] = args.low_pass
        self.parameters['high_pass'] = args.high_pass
        self.parameters['thresholds'] = args.thresholds

        self.probe = args.probe

    def find_datasets(self, subject_name = None, session_name = None):
        '''
        Searches for subjects and sessions in EphysRecording
        '''
        if subject_name is None and session_name is None:
            print("\n\nPlease specify a 'subject_name' and a 'session_name' to perform spike-sorting.\n\n")
        keys = []
        if not subject_name is None:
            if len(subject_name) > 1:
                raise ValueError(f'Please submit one subject at a time {subject_name}.')
            if not subject_name[0] == '':
                subject_name = subject_name[0]
        if not session_name is None:
            for s in session_name:
                if not s == '':
                    keys.append(dict(subject_name = subject_name,
                                     session_name = s))
        else:
            keys.append(dict(subject_name = subject_name))
        from ..schema import EphysRecording
        datasets = []
        for k in keys:
            datasets += (EphysRecording()& k).proj('subject_name','session_name','dataset_name').fetch(as_dict = True)
        return datasets
        
    def _compute(self):
        from ..schema import EphysRecording
        datasets = pd.DataFrame((EphysRecording.ProbeFile() & self.dataset_key).fetch())

        for probe_num in np.unique(datasets.probe_num):
            self.set_job_status(job_log = f'Sorting {probe_num}')
            files = datasets[datasets.probe_num.values == probe_num]
            dset = []
            for i,f in files.iterrows():
                if 'ap.cbin' in f.file_path:
                    dset.append(i)
            dset = files.loc[dset]
            if not len(dset):
                print(files)
                raise(ValueError(f'Could not find ap.cbin files for probe {probe_num}'))
            
            localfiles = self.get_files(dset, allowed_extensions = ['.ap.bin'])
            probepath = list(filter(lambda x: str(x).endswith('bin'),localfiles))
            if self.parameters['algorithm_name'] == 'spks_kilosort2.5':      
                from spks.sorting import ks25_run
                results_folder = ks25_run(sessionfiles = probepath,
                                          temporary_folder = prefs['scratch_path'],
                                          do_post_processing = False,
                                          motion_correction = self.parameters['motion_correction'],
                                          thresholds = self.parameters['thresholds'],
                                          lowpass = self.parameters['low_pass'],
                                          highpass = self.parameters['high_pass'])
            elif self.parameters['algorithm_name'] == 'spks_kilosort4':      
                from spks.sorting import ks4_run
                results_folder = ks4_run(sessionfiles = probepath,
                                         temporary_folder = prefs['scratch_path'],
                                         do_post_processing = False,
                                         motion_correction = self.parameters['motion_correction'],
                                         thresholds = self.parameters['thresholds'],
                                         lowpass = self.parameters['low_pass'],
                                         highpass = self.parameters['high_pass'])
            elif self.parameters['algorithm_name'] == 'spks_mountainsort5':
                raise(NotImplemented(f"Algorithm {self.parameters['algorithm_name']} not implemented."))
            else:
                raise(NotImplemented(f"Algorithm {self.parameters['algorithm_name']} not implemented."))
            self.set_job_status(job_log = f'Probe {probe_num} sorted, running post-processing.')
            self.postprocess_and_insert(results_folder,
                                        probe_num = probe_num,
                                        remove_duplicates = True,
                                        n_pre_samples = 45)
                
    def postprocess_and_insert(self,
                               results_folder,
                               probe_num,
                               remove_duplicates = True,
                               n_pre_samples = 45):
        '''Does the preprocessing for a spike sorting and inserts'''
        from spks import Clusters
        if remove_duplicates:
            clu = Clusters(results_folder, get_waveforms = False, get_metrics = False)
            clu.remove_duplicate_spikes(overwrite_phy = True) 
            del clu
        clu = Clusters(results_folder, get_waveforms = False, get_metrics = False)
        clu.compute_template_amplitudes_and_depths()
        # waveforms
        
        base_key = dict(self.dataset_key,
                        probe_num = probe_num,
                        parameter_set_num = self.parameter_set_num)
        ssdict = dict(base_key,
                      n_pre_samples = n_pre_samples,
                      n_sorted_units = len(clu),
                      n_detected_spikes = len(clu.spike_times),
                      sorting_datetime = datetime.fromtimestamp(
                          Path(results_folder).stat().st_ctime),
                      channel_indices = clu.channel_map.flatten(),
                      channel_coords = clu.channel_positions)
        udict = [] # unit
        for iclu in clu.cluster_id:
            idx = np.where(clu.spike_clusters == iclu)[0]
            udict.append(dict(base_key,unit_id = iclu,
                              spike_positions = clu.spike_positions[idx,:].astype(np.float32),
                              spike_times = clu.spike_times[idx].flatten().astype(np.uint64),
                              spike_amplitudes = clu.spike_amplitudes[idx].flatten().astype(np.float32)))
        
        featurestosave = dict(template_features = clu.spike_pc_features.astype(np.float32),
                              spike_templates = clu.spike_templates,
                              cluster_indices = clu.spike_clusters,
                              whitening_matrix = clu.whitening_matrix,
                              templates = clu.templates,
                              template_feature_ind = clu.template_pc_features_ind)
        # save the features to a file, will take like 2 min
        save_dict_to_h5(Path(results_folder)/'features.hdf5',featurestosave)
        n_jobs = DEFAULT_N_JOBS  # gets the default number of jobs from labdata
        # extract the waveforms 
        udict = select_random_waveforms(udict, wpre = n_pre_samples, wpost = n_pre_samples)
        from tqdm import tqdm
        binaryfile = list(Path(results_folder).glob("filtered_recording*.bin"))[0]
        nchannels = clu.metadata['nchannels'] 
        res = get_waveforms_from_binary(binaryfile, nchannels, [u['waveform_indices'] for u in udict],
                                        wpre = n_pre_samples,
                                        wpost = n_pre_samples,
                                        n_jobs = n_jobs)
        
        # utemp = get_waveforms_from_binary(binaryfile,nchannels,[u['waveform_indices'] for u in udict])
        median_waveforms = Parallel(backend='loky', n_jobs = n_jobs)(
            delayed(lambda x: np.median(x.astype(np.float32),axis = 0))(r) for r in tqdm(res))
        tosave = {}
        waves_dict = []
        for u,w,m in zip(udict,res,median_waveforms):
            waves_dict.append(dict(base_key,
                                   unit_id = u['unit_id'],
                                   waveform_median = m*clu.channel_gains))
            tosave[str(u['unit_id'])] = dict(waveforms = w, indices = u['waveform_indices'])
        # this takes roughly 7 min per dataset because of the compression...
        save_dict_to_h5(Path(results_folder)/'waveforms.hdf5',tosave) 

        src = [Path(results_folder)/'waveforms.hdf5',Path(results_folder)/'features.hdf5']
        dataset = dict(**self.dataset_key)
        dataset['dataset_name'] = f'spike_sorting/{self.parameter_set_num}'
        from ..schema import AnalysisFile
        filekeys = AnalysisFile().upload_files(src,dataset)
        ssdict['waveforms_file'] = filekeys[0]['file_path']
        ssdict['waveforms_storage'] = filekeys[0]['storage']
        ssdict['features_file'] = filekeys[1]['file_path']
        ssdict['features_storage'] = filekeys[1]['storage']
        # insert the syncs
        events = []
        stream_name = f'imec{probe_num}'
        for c in clu.metadata.keys():
            if 'sync_onsets' in c:
                for k in clu.metadata[c].keys():
                    events.append(dict(self.dataset_key,
                                       stream_name = stream_name,
                                       event_name = str(k),
                                       event_values = clu.metadata[c][k].astype(np.uint64)))
        from ..schema import SpikeSorting, SpikeSortingParams, EphysRecording, DatasetEvents
        if len(events):
            # Add stream
            DatasetEvents.insert1(dict(self.dataset_key,
                                       stream_name = stream_name),
                                       skip_duplicates = True, allow_direct_insert = True)
            DatasetEvents.Digital.insert(events,
                                         skip_duplicates = True,
                                         allow_direct_insert = True)
    
        # inserts
        # do all the inserts here
        import logging
        logging.getLogger('datajoint').setLevel(logging.WARNING)
        # these can't be done in a safe way quickly so if they fail we have delete SpikeSorting
        SpikeSorting.insert1(ssdict,skip_duplicates = True)
        # Insert datajoint in parallel.
        Parallel(n_jobs = n_jobs)(delayed(SpikeSorting.Unit.insert1)(
            u,
            skip_duplicates=True,
            ignore_extra_fields = True) for u in tqdm(udict));
        Parallel(n_jobs = n_jobs)(delayed(SpikeSorting.Waveforms.insert1)(
            u,
            skip_duplicates=True,
            ignore_extra_fields = True) for u in tqdm(waves_dict));
        # Add a segment from a random location.
        from spks.io import map_binary
        dat = map_binary(binaryfile,nchannels = nchannels)
        nsamples = int(clu.sampling_rate*2)
        offset_samples = int(np.random.uniform(nsamples, len(dat)-nsamples-1))
        SpikeSorting.Segment.insert1(dict(base_key,
                                          segment_num = 1,
                                          offset_samples = offset_samples,
                                          segment = np.array(dat[offset_samples : offset_samples + nsamples])))
        del dat
        self.set_job_status(job_log = f'Completed {base_key}')
        
def select_random_waveforms(unit_dict,
                            wpre = 45,
                            wpost = 45,
                            nmax_waves = 500):
    
    duration = np.max([np.max(u['spike_times']) for u in unit_dict])
    for u in unit_dict:
        s = u['spike_times']
        s_begin = s[(s>(wpre+2))&(s<(duration//4))]
        s_end = s[(s>(3*(duration//4))) & (s<(duration-wpost-2))]
        sel = []
        if len(s_begin)>nmax_waves:
            sel = [t for t in np.random.choice(s_begin, nmax_waves, replace=True)]
        else:
            sel = [t for t in s_begin]
        if len(s_end)>nmax_waves:
            sel += [t for t in np.random.choice(s_end, nmax_waves, replace=True)]
        else:
            sel += [t for t in s_end]
        u['waveform_indices'] = np.sort(np.array(sel).flatten()) # add this to the  
    return unit_dict

def get_spike_waveforms(data,indices,wpre = 45,wpost = 45):
    idx = np.arange(-wpre,wpost,dtype = np.int64)
    waves = []
    for i in indices.astype(np.int64):
        waves.append(np.array(np.take(data,idx+i,axis = 0)))
    return np.stack(waves,dtype = data.dtype)

def get_waveforms_from_binary(binary_file,
                              binary_file_nchannels,
                              waveform_indices,
                              wpre = 45,
                              wpost = 45,
                              n_jobs = 8):
    from tqdm import tqdm
    from spks.io import map_binary
    dat = map_binary(binary_file,nchannels = binary_file_nchannels)
    res = Parallel(backend='loky',n_jobs=n_jobs)(delayed(get_spike_waveforms)(
        dat,
        w,
        wpre = wpre,
        wpost = wpost) for w in tqdm(
            waveform_indices,desc = "Extracting waveforms"))
    return res
