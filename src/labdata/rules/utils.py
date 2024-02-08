from ..utils import *

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
    Returns a list of true/false the same size as paths
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

class UploadRule():
    def __init__(paths, checksums = None, use_db = True):
        '''

Rule to apply on upload. 

        1) Checksum on the files; compare with provided (reserve job if use_db)
        2) Apply function
        3) Checksum on the output - the files that changed
        4) Submit upload
        5) Update tables 

Can submit job on slurm, some of these can be long or take resources.

        '''
        self.job_id = None
        self.src_paths = None
        # parse inputs
        if use_db:
            from ..schema import UploadJob
        if not type(paths) is list:
            # then it is a list of paths
            if type(paths) is int:
                # then it is a jobid?
                self.job_id = paths
                if not use_db:
                    raise ValueError('use_db has to be set to input a jobid as path')
                # get the paths
                tmp = (UploadJob.AssignedFiles() & dict(job_id = self.job_id)).fetch('src_path','src_md5')
                paths = [f[0] for f in tmp]
        else:
            # get the job_id from the paths
            if use_db:
                for path in paths:
                    query = (UploadJob.AssignedFiles() & dict(src_path = path)).fetch(as_dict = True)
                    if not len(query):
                        raise ValueError(f'Could not find {path} in UploadJob.AssignedFiles.')
                    self.job_id = query[0]['job_id']  # get the job id
        # check the job status
        self.src_paths = paths
        if not self.job_id is None:
            self.jobquery = (UploadJob() & f'job_id = self.job_id')
            job_status = query.fetch(as_dict = True)[0]
            if job_status['job_waiting']:
                self.jobquery.update1(dict(job_waiting = False,
                                           job_host = pref['hostname'])) # take the job
            else:
                print("Job is already taken.")
                print(job_status, flush = True)
                return
                
        # this should not fail because we have to keep track of errors, should update the table
        if not compare_md5s(paths,checksums):
            print('CHECKSUM FAILED for {0}'.format(Path(paths[0]).parent))
            if not self.job_id is None:
                # recomputing md5s
                self.jobquery.update1(dict(job_host = None,
                                           job_log = 'MD5 CHECKSUM failed; check file transfer.'))
                print(f'Check job_id {self.job_id}')

        self.original_paths = [p for p in 
        self.apply_rule()

        

            
