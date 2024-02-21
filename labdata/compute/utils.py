from ..utils import *

class Compute():
    def __init__(self,job_id):
        '''
        Executes a computation on a dataset, that can be remote or local
        Uses a singularity image if possible
        '''
        self.name = 'computejob'
        
        self.job_id = job_id
        self.singularity_image = 'labdata-base'
        self.paths = None
        self.local_path = prefs['local_paths'][0]
        self.dataset_key = None
        self.is_singularity = False
        self.is_ec2 = False # then files should be taken from s3

    def run_on_container(self):
        pass
        
    def compute(self):
        '''This actually does the computation; gets the data and runs it.'''
        from ..schema import ComputeJob, File, dj
        
        if not self.job_id is None:
            with dj.conn().transaction:
                self.jobquery = (ComputeJob() & dict(job_id = self.job_id))
                job_status = self.jobquery.fetch(as_dict = True)
                if len(job_status):
                    if job_status[0]['job_waiting']:
                        self.set_job_status(job_status = 'RUNNING', job_waiting = 0) # take the job
                    else:
                        print(f"Job {self.job_id} is already taken.")
                        print(job_status, flush = True)
                        return # exit.
                else:
                    raise ValueError(f'job_id {self.job_id} does not exist.')
        # get the paths
        self.src_paths = pd.DataFrame((ComputeJob.AssignedFiles() & dict(job_id = self.job_id)).fetch())
        if not len(self.src_paths):
            self.set_job_status(job_status = 'FAILED',
                                job_log = f'Could not find files for {self.job_id} in ComputeJob.AssignedFiles.')
            raise ValueError(f'Could not find files for {self.job_id} in ComputeJob.AssignedFiles.')
        
        try:
            self._compute() # can use the src_paths
        except Exception as err:
            # log the error
            print('There was an error processing this dataset.')
            print(err)
            self.set_job_status(job_status = 'FAILED',job_log = f'{err}')
            return
        self._post_compute() # so the rules can insert tables and all.
        
    def set_job_status(self, job_status = 'FAILED',job_log = '',job_waiting = 0):
        from ..schema import UploadJob
        if not self.job_id is None:
            # recomputing md5s
            ComputeJob.update1(dict(job_id = self.job_id,
                                    job_waiting = job_waiting,
                                    job_host = prefs['hostname'], # write the hostname so we know where it failed.
                                    job_status = job_status,
                                    job_log = job_log))
            print(f'Check job_id {self.job_id} : {job_status}')

    def _post_compute(self):
        return
    
    def _compute(self):
        # this rule does nothing, so the src_paths are going to be empty,
        # and the "paths" are going to be the src_paths
        self.processed_paths = None # processed paths are just the same, no file changed, so no need to do anything.
        # needs to compute the checksum on all the new files
        
