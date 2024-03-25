from ..utils import *
import traceback

def load_analysis_object(analysis):
    if not analysis in prefs['compute']['analysis'].keys():
        print(f'\n\nCould not find [{analysis}] analysis.\n\t The analysis are {list(prefs["compute"]["analysis"].keys())}\n\n')
        raise ValueError('Add the analysis to the "compute" section of the preference file {analysis_name:analysis_object}.\n\n')
        
    import labdata
    return eval(prefs['compute']['analysis'][analysis])

def handle_compute(job_id):
    from ..schema import ComputeTask
    jobinfo = pd.DataFrame((ComputeTask() & dict(job_id = job_id)).fetch())
    if not len(jobinfo):
        print(f'No task with id: {job_id}')
    jobinfo = jobinfo.iloc[0]
    if not jobinfo.task_waiting:
        print(f'Task {job_id} is running on {jobinfo.task_host}')
    obj = load_analysis_object(jobinfo.task_name)(jobinfo.job_id)
    return obj

def parse_analysis(analysis, job_id = None,
                   subject = [''],
                   session = [''],
                   secondary_args = None,
                   full_command = None,
                   launch_singularity = False,
                   **kwargs):

    obj = load_analysis_object(analysis)(job_id)
    obj.secondary_parse(secondary_args)
    if obj.job_id is None:
        # then we have to create jobs and assign
        from ..schema import ComputeTask
         # first check if there is a task that has already been submitted with the exact same command.
         # this has a caveat: if the order of the arguments is switched, it wont work..
        submittedjobs = (ComputeTask() & dict(task_cmd = full_command))
        if len(submittedjobs):
            print('A similar job is already submitted:')
            #print(submittedjobs)
            return
        datasets = obj.find_datasets(subject_name = subject,session_name = session)        
        job_ids = obj.place_tasks_in_queue(datasets,task_cmd = full_command)
        # now we have the job ids, need to figure out how to launch the jobs
        print(job_ids)
    print(obj)
        
# this class will execute compute jobs, it should be independent from the CLI but work with it.
class BaseCompute():
    def __init__(self,job_id, allow_s3 = None):
        '''
        Executes a computation on a dataset, that can be remote or local
        Uses a singularity image if possible
        '''
        self.name = 'computejob'
        self.file_filters = ['.'] # selects all files...
        self.parameters = dict()
        
        self.job_id = job_id
        self.container = 'labdata-base'
        if not self.job_id is None:
            self._check_if_taken()
            
        self.paths = None
        self.local_path = Path(prefs['local_paths'][0])
        self.scratch_path = Path(prefs['scratch_path'])
        self.assigned_files = None
        self.dataset_key = None
        self.is_container = False
        if allow_s3 is None:
            self.allow_s3 = prefs['allow_s3_download']
        if 'LABDATA_CONTAINER' in os.environ.keys():
            # then it is running inside a container
            self.is_container = True
        #self.is_ec2 = False # then files should be taken from s3

    def _init_job(self): # to run in the init function
        if not self.job_id is None:
            from ..schema import ComputeTask,dj
            with dj.conn().transaction:
                self.jobquery = (ComputeTask() & dict(job_id = self.job_id))
                job_status = self.jobquery.fetch(as_dict = True)
                if len(job_status):
                    if not job_status[0]['task_waiting']:
                        print(f"Compute task [{self.job_id}] is already taken.")
                        print(job_status, flush = True)
                        return # exit.
                    else:
                        self.set_job_status(job_status = 'WORKING',
                                            job_waiting = 0)
                        par = json.loads(job_status[0]['task_parameters'])
                        for k in par.keys():
                            self.parameters[k] = par[k]
                        self.assigned_files = pd.DataFrame((ComputeTask.AssignedFiles() & dict(job_id = self.job_id)).fetch())
                        self.dataset_key = dict(subject_name = job_status[0]['subject_name'],
                                                session_name = job_status[0]['session_name'],
                                                dataset_name = job_status[0]['dataset_name'])
                else:
                    # that should just be a problem to fix
                    raise ValueError(f'job_id {self.job_id} does not exist.')

    def get_files(self, dset, allowed_extensions=[]):
        '''
        Gets the paths and downloads from S3 if needed.
        '''
        
        files = dset.file_path.values
        storage = dset.storage.values
        localpath = Path(prefs['local_paths'][0])
        self.files_existed = True
        localfiles = np.unique([find_local_filepath(f,
                                                    allowed_extensions = allowed_extensions) for f in files])
        if not len(localfiles):
            # then you can try downloading the files
            if self.allow_s3: # get the files from s3
                from ..s3 import copy_from_s3
                for s in np.unique(storage):
                    # so it can work with multiple storages
                    srcfiles = files[storage == s]
                    dstfiles = [localpath/f for f in srcfiles]
                    copy_from_s3(srcfiles,dstfiles,storage = s)
                localfiles = np.unique([find_local_filepath(f,
                                                            allowed_extensions = allowed_extensions) for f in files])
                if len(localfiles): self.files_existed = False # delete the files in the end if they were not local.
            else:
                print(files, localpath)
                raise(ValueError('Files not found locally, set allow_s3 in the preferences to download.'))
        return localfiles


    def place_tasks_in_queue(self,datasets,task_cmd = None):
        ''' This will put the tasks in the queue for each dataset.
        If the task and parameters are the same it will return the job_id instead.
        '''
        from ..schema import ComputeTask, Dataset,dj
        job_ids = []
        for dataset in datasets:
            files = pd.DataFrame((Dataset.DataFiles() & dataset).fetch())
            idx = []
            for f in self.file_filters:
                idx += list(filter(lambda x: not x is None,[i if f in s else None for i,s in enumerate(
                    files.file_path.values)]))
            if len(idx) == 0:
                raise ValueError(f'Could not find valid Dataset.DataFiles for {dataset}')
            files = files.iloc[idx]
            key = dict(dataset,task_name = self.name) 
            exists = ComputeTask() & key
            if len(exists):
                d = pd.DataFrame(exists.fetch())
                if len(d.task_parameters.values == json.dumps(self.parameters)):
                    job_id = d[d.task_parameters.values == json.dumps(self.parameters)].job_id.iloc[0]
                    print(f'There is a task to analyse dataset {key} with the same parameters. [{job_id}]')
                    job_ids.append(job_id)
            else:
                with dj.conn().transaction:
                    job_id = ComputeTask().fetch('job_id')
                    if len(job_id):
                        job_id = np.max(job_id) + 1 
                    else:
                        job_id = 1
                    ComputeTask().insert1(dict(key,
                                               job_id = job_id,
                                               task_waiting = 1,
                                               task_status = "WAITING",
                                               task_target = None,
                                               task_host = None,
                                               task_cmd = task_cmd,
                                               task_parameters = json.dumps(self.parameters),
                                               task_log = None))
                    ComputeTask.AssignedFiles().insert([dict(job_id = job_id,
                                                             storage = f.storage,
                                                             file_path = f.file_path)
                                                        for i,f in files.iterrows()])
                    job_ids.append(job_id)
        return job_ids
    
    def find_datasets(self,subject_name = None, session_name = None, dataset_name = None):
        '''
        Find datasets to analyze, this function will search in the proper tables if datasets are available.
        Has to be implemented per Compute class since it varies.
        '''
        raise NotImplemented('The find_datasets method has to be implemented.')
        
    def secondary_parse(self,secondary_arguments):
        if secondary_arguments is None:
            return
        else:
            self._secondary_parse(secondary_arguments)
        
    def _secondary_parse(self,secondary_arguments):
        print(f'\n\nThere are no secondary arguments {secondary_arguments} for this analysis.\n\n')
        
    def run_on_container(self):
        # this should just start a container and run the same command that was used to run this on the CLI
        pass
    def run_on_ec2(self):
        # this has to:
        #   1. start an instance
        #   2. connect through ssh to the instance
        #   3. setup the labdata environment, plugins and container
        #   4. launch the job.
        
        import boto3
        commands = [' echo "hello world"']
        ssm_client = boto3.client('ssm')
        
        output = ssm_client.send_command(
            InstanceIds=["i-your_instance_id"],
            DocumentName='AWS-RunShellScript',
            Parameters={
                'commands': commands
            })

    def _check_if_taken(self):
        if not self.job_id is None:
            from ..schema import ComputeTask, File, dj
            self.jobquery = (ComputeTask() & dict(job_id = self.job_id))
            job_status = self.jobquery.fetch(as_dict = True)
            if len(job_status):
                if job_status[0]['task_waiting']:
                    return
                else:
                    print(job_status, flush = True)
                    raise ValueError(f'job_id {self.job_id} is already taken.')
                    return # exit.
            else:
                raise ValueError(f'job_id {self.job_id} does not exist.')
            # get the paths
            self.src_paths = pd.DataFrame((ComputeTask.AssignedFiles() &
                                           dict(job_id = self.job_id)).fetch())
            if not len(self.src_paths):
                self.set_job_status(job_status = 'FAILED',
                                    job_log = f'Could not find files for {self.job_id} in ComputeTask.AssignedFiles.')
                raise ValueError(f'Could not find files for {self.job_id} in ComputeTask.AssignedFiles.')
        else:
            raise ValueError(f'Compute: job_id not specified.')
        
    def compute(self):
        '''This calls the compute function. If "use_s3" is true it will download the files from s3 when needed.'''
        try:
            self._compute() # can use the src_paths
        except Exception as err:
            # log the error
            print(f'There was an error processing job {self.job_id}.')
            err =  str(traceback.format_exc()) + "ERROR" +str(err)
            print(err)

            if len(err) > 1999: # then get only the last part of the error.
                err = err[-1900:]
            self.set_job_status(job_status = 'FAILED',job_log = f'{err}')
            return
        self._post_compute() # so the rules can insert tables and all.
        # get the job from the DB if the status is not failed, mark completed (remember to clean the log)
        from ..schema import ComputeTask
        self.jobquery = (ComputeTask() & dict(job_id = self.job_id))
        job_status = self.jobquery.fetch(as_dict = True)
        if not job_status[0]['task_status'] in ['FAILED']:
            self.set_job_status(job_status = 'COMPLETED')
            
    def set_job_status(self, job_status = None, job_log = None,job_waiting = 0):
        from ..schema import ComputeTask
        if not self.job_id is None:
            dd = dict(job_id = self.job_id,
                      task_waiting = job_waiting,
                      task_host = prefs['hostname']) # so we know where it failed.)
            if not job_status is None:
                dd['task_status'] = job_status
            if not job_log is None:
                dd['task_log'] = job_log
            ComputeTask.update1(dd)
            if not 'WORK' in job_status: # display the message
                print(f'Check job_id {self.job_id} : {job_status}')

    def _post_compute(self):
        '''
        Inserts the data to the database
        '''
        return
    
    def _compute(self):
        '''
        Runs the compute job on a scratch folder.
        '''
        return
    
