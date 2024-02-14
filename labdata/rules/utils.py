from ..utils import *
from ..s3 import copy_to_s3

# has utilities needed by other rules

def check_if_rule_in_path(paths,rule):
    '''
    Checks if a rule applies to any of the files paths.
    Returns a list of true/false the same size as paths
    
    '''
    res = [False for p in paths]
    if rule is None:
        return res
    for i,p in enumerate(paths): 
        if '*' in rule:
            res[i] = all([a in p for a in filter(len,rule.split('*'))])
        elif len(rule) and rule in str(p):
            res[i] = True
            
    return res


def check_if_rules_apply(paths,rules = None):
    '''
    Checks if a rule applies to any of the files paths.
    Returns a list of true/false the same size as paths.
    '''
    if rules is None:
        if 'upload_rules' in prefs.keys():
            rules = prefs['upload_rules']
        else:
            print('No rules to check. Adjust preferences.')
            return None, [False for f in paths]
    for rulekey in rules.keys():
        rule = rules[rulekey]
        match = check_if_rule_in_path(paths, rule['rule'])
        if np.sum(match)>0:
            print(f'Dataset follows rule {rulekey}')
            return rule, match
    return None, [False for f in paths]

def _checksum_files(filepath, local_path):
    '''
    Checksum files that don't need to be copied
    '''
    # construct the path:
    src = Path(local_path)/filepath
    hash = compute_md5_hash(src)  # computes the hash
    srcstat = src.stat()
    file_size = srcstat.st_size
    return dict(src_path = filepath,
                src_md5 = hash,
                src_size = file_size,
                src_datetime = datetime.fromtimestamp(srcstat.st_ctime))

class UploadRule():
    def __init__(self,job_id):
        '''

Rule to apply on upload. 

        1) Checksum on the files; compare with provided (reserve job if use_db)
        2) Apply function
        3) Checksum on the output - the files that changed
        4) Submit upload
        5) Update tables 

Can submit job on slurm, some of these can be long or take resources.

        '''
        self.rule_name = 'default'
        
        self.job_id = job_id
        self.src_paths = None
        self.processed_paths = None
        self.dst_paths = None
        self.local_path = prefs['local_paths'][0]
        self.dataset_key = None # will get written on upload, use in _post_upload
        
    def apply(self):
        # parse inputs
        from ..schema import UploadJob, File, dj

        if not self.job_id is None:
            with dj.conn().transaction:
                self.jobquery = (UploadJob() & dict(job_id = self.job_id))
                job_status = self.jobquery.fetch(as_dict = True)
                if len(job_status):
                    if job_status[0]['job_waiting']:
                        self.set_job_status(job_status = 'WORKING', job_waiting = 0) # take the job
                    else:
                        print(f"Job {self.job_id} is already taken.")
                        #print(job_status, flush = True)
                        return # exit.
                else:
                    raise ValueError(f'job_id {self.job_id} does not exist.')
        # get the paths
        self.src_paths = pd.DataFrame((UploadJob.AssignedFiles() & dict(job_id = self.job_id)).fetch())
        if not len(self.src_paths):
            self.set_job_status(job_status = 'FAILED',
                                job_log = f'Could not find files for {self.job_id} in Upload.AssignedFiles.')
            raise ValueError(f'Could not find files for {self.job_id} in Upload.AssignedFiles.')
        self.upload_storage = self.jobquery.fetch('upload_storage')[0]

        
        # this should not fail because we have to keep track of errors, should update the table
        src = [Path(self.local_path) / p for p in self.src_paths.src_path.values] 
        if not compare_md5s(src,self.src_paths.src_md5.values):
            print('CHECKSUM FAILED for {0}'.format(Path(self.src_paths.src_path.iloc[0]).parent))
            self.set_job_status(job_status = 'FAILED',job_log = 'MD5 CHECKSUM failed; check file transfer.')
            return # exit.
        try:
            self._apply_rule() # can use the src_paths
        except Exception as err:
            # log the error
            print('There was an error processing this dataset.')
            print(err)
            self.set_job_status(job_status = 'FAILED',job_log = f'{err}')
            return
        # compare the hashes after
        self._upload()
        self._post_upload() # so the rules can insert tables and all.
        
    def set_job_status(self, job_status = 'FAILED',job_log = '',job_waiting = 0):
        from ..schema import UploadJob
        if not self.job_id is None:
            # recomputing md5s
            UploadJob.update1(dict(job_id = self.job_id,
                                   job_waiting = job_waiting,
                                   job_host = prefs['hostname'], # write the hostname so we know where it failed.
                                   job_status = job_status,
                                   job_log = job_log))
            print(f'Check job_id {self.job_id} : {job_status}')

    def _handle_processed_and_src_paths(self, processed_files,new_files):
        '''
        Put the files in the proper place and compute checksums for new files.
        Call this from the apply method.
        '''
        n_jobs = DEFAULT_N_JOBS
        self.processed_paths = []
        for f in processed_files:
            i = np.where(self.src_paths.src_path == f)[0][0]
            self.processed_paths.append(self.src_paths.iloc[i])
            self.src_paths.drop(self.src_paths.iloc[i].name,axis = 0,inplace = True)
            self.src_paths.reset_index(drop=True,inplace = True)
        self.processed_paths = pd.DataFrame(self.processed_paths).reset_index(drop=True)        

        res = Parallel(n_jobs = n_jobs)(delayed(_checksum_files)(
            path,
            local_path = self.local_path) for path in new_files)
        for r in res:
            r['job_id'] = self.job_id
        self.src_paths = pd.concat([self.src_paths,pd.DataFrame(res)], ignore_index=True)

    def _post_upload(self):
        return
    
    def _upload(self):
        # this reads the attributes and uploads
        # It also puts the files in the Tables
        
        # destination in the bucket is actually the path
        dst = [k for k in self.src_paths.src_path.values]
        # source is the place where data are
        src = [Path(self.local_path) / p for p in self.src_paths.src_path.values] # same as md5
        # s3 copy in parallel hashes were compared before so no need to do it now.
        copy_to_s3(src,dst,md5_checksum=None,storage_name=self.upload_storage)
        from ..schema import UploadJob, File, dj, ProcessedFile, Dataset
        with dj.conn().transaction:  # make it all update at the same time
            # insert to Files so we know where to get the data
            ins = []
            for i,f in self.src_paths.iterrows():
                ins.append(dict(file_path = f.src_path,
                                storage = self.upload_storage,
                                file_datetime = f.src_datetime,
                                file_size = f.src_size,
                                file_md5 = f.src_md5))

            File.insert(ins)
            # Add to dataset?
            job = self.jobquery.fetch(as_dict=True)[0]
            # check if it has a dataset
            if all([not job[a] is None for a in ['subject_name','session_name','dataset_name']]):
                for i,p in enumerate(ins):
                    ins[i] = dict(subject_name = job['subject_name'],
                                  session_name = job['session_name'],
                                  dataset_name = job['dataset_name'],
                                  file_path = p['file_path'],
                                  storage = self.upload_storage)
                Dataset.DataFiles.insert(ins)
                self.dataset_key = dict(subject_name = job['subject_name'],
                                        session_name = job['session_name'],
                                        dataset_name = job['dataset_name'])
            # Insert the processed files so the deletions are safe
            if not self.processed_paths is None:
                ins = []
                for i,f in self.processed_paths.iterrows():
                    ins.append(dict(file_path = f.src_path,
                                    file_datetime = f.src_datetime,
                                    file_size = f.src_size,
                                    file_md5 = f.src_md5))
                ProcessedFile.insert(ins)
            (UploadJob & dict(job_id = self.job_id)).delete(safemode = False)
            # completed
        
    def _apply_rule(self):
        # this rule does nothing, so the src_paths are going to be empty,
        # and the "paths" are going to be the src_paths
        self.processed_paths = None # processed paths are just the same, no file changed, so no need to do anything.
        # needs to compute the checksum on all the new files
        
