from .utils import *
import argparse

class CLI_parser():
    def __init__(self):
        parser = argparse.ArgumentParser(
            description = 'labdata - tools to manage data in an experimental neuroscience lab',
            usage = ''' labdata <command> [args]
Data manipulation commands are:

            subjects                            List subjects
            sessions -a <subject>               List sessions 
            get -a <subject> -s <session>       Download data from one session if not already there
            put -a <subject> -s <session>       Copies a dataset to the server to be used
            clean                               Deletes files that are already added

Data analysis commands:

            run <analysis> -a <subject> -s <session>        Allocates and runs analysis, local, queued or on AWS
            task <compute_task_number>                      
            
Server commands (don't run on experimental computers):
            upload                                          Sends pending data to S3 (applies upload rules)
            ''')
        parser.add_argument('command', help= 'type: labdata2 <command> -h for help')

        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print('The command [{0}] was not recognized. '.format(args.command))
            parser.print_help()
            exit(1)
        getattr(self,args.command)()  # Runs the following parser

    def sessions(self):
        parser = argparse.ArgumentParser(
            description = 'List sessions and datatypes',
            usage = '''labdata sessions -a <SUBJECT>''')
        parser = self._add_default_arguments(parser,1)
            
        args = parser.parse_args(sys.argv[2:])
        from .schema import Subject, Session, Dataset
        for s in args.subject:
            subject_name = s
            datasets = pd.DataFrame((Dataset()*Session() &
                                     dict(subject_name = subject_name)).fetch())
            sessions = np.sort(np.unique(datasets.session_datetime.values))
            print(f'\n {s} - {len(sessions)} sessions - {len(datasets)} datasets')
            for ses in sessions:
                dsets = datasets[datasets.session_datetime == ses]
                print(f'\t {dsets.iloc[0].session_name}')
                for i,t in dsets.iterrows():
                    if t.dataset_type is None:
                        print(f'\t\t *{t.dataset_name}')
                    else:
                        print(f'\t\t {t.dataset_type} - {t.dataset_name}')
                        
    def put(self):
        parser = argparse.ArgumentParser(
            description = 'Copies data to the server to be uploaded',
            usage = '''labdata put -a <SUBJECT> -s <SESSION>''')
        parser = self._add_default_arguments(parser)
        parser.add_argument('-t','--datatype-name',
                            action='store',
                            default=dataset_type_names, type=str,nargs=1)

            
        args = parser.parse_args(sys.argv[2:])

        if not len(args.subject[0]) or not len(args.session[0]):
            from .widgets import QApplication, LABDATA_PUT
            app = QApplication(sys.argv)
            w = LABDATA_PUT()
            sys.exit(app.exec_())
 
    def run(self):
        parser = argparse.ArgumentParser(
            description = 'Allocates or runs an analysis',
            usage = '''labdata run <ANALYSIS> -a <SUBJECT> -s <SESSION>''')
        parser.add_argument('analysis',action = 'store',default = '',type = str)
        parser.add_argument('-j','--job',action = 'store',default = None, type = int)
        parser.add_argument('-t','--target',action = 'store',default = prefs['compute']['default_target'], type = str)
        
        parser = self._add_default_arguments(parser)
        secondary_args = None
        argum = sys.argv[2:]
        if '--' in sys.argv:
            argum = sys.argv[2:sys.argv.index('--')]
            secondary_args = sys.argv[sys.argv.index('--'):]
        args = parser.parse_args(argum)
        from .compute import parse_analysis
        # parse analysis will check if the analysis is defined
        jobids,container,cuda,name = parse_analysis(analysis = args.analysis,
                                                    job_id = args.job, 
                                                    subject = args.subject,
                                                    session = args.session,
                                                    datatype = args.datatype,
                                                    secondary_args = secondary_args,
                                                    full_command = ' '.join(sys.argv[1:]))
        from .compute.singularity import run_on_singularity
        container_file = (Path(prefs['compute']['containers']['local'])/container).with_suffix('.sif')
        cmds = []
        for j in jobids:
            if container_file.exists():
                cmds.append(run_on_singularity(container_file,command = f'labdata2 task {j}',
                                               cuda = cuda,
                                               bind_from_prefs = True,
                                               dry_run = True))
            else:
                cmds.append(f'labdata2 task {j}')
        if args.target == 'slurm':
            from .compute.schedulers import slurm_exists,slurm_submit
            if slurm_exists():
                for cmd in cmds:
                    slurm_submit(name,
                                 cmd,
                                 ntasks = 1,
                                 ncpuspertask = DEFAULT_N_JOBS, # change later
                                 gpus = 1 if cuda else None)
            else:
                print('Could not find SLURM: did not submit compute tasks:')
                print('\t\n'.join(cmds))
            
    def task(self):
        parser = argparse.ArgumentParser(
            description = 'Runs a ComputeTask',
            usage = '''labdata task <JOB_ID> ''')
        parser.add_argument('job_id',action = 'store',default = None,type = int)
        args = parser.parse_args(sys.argv[2:])
        job_id = args.job_id
        if not job_id is None:
            from .compute import handle_compute
            task = handle_compute(job_id)
            task.compute()
        
    def _add_default_arguments(self, parser,level = 3):
        if level >= 1:
            parser.add_argument('-a','--subject',
                                action='store',
                                default=None, type=str,nargs='+')
        if level >= 2:

            parser.add_argument('-s','--session',
                                action='store',
                                default=None, type=str,nargs='+')
        if level >= 3:
            parser.add_argument('-d','--datatype',
                            action='store',
                            default=None, type=str,nargs='+')
        return parser
            
    def _get_default_arg(self,argument,cli_arg = 'submit', default = None):
        # checks if there is a default in the options
        if not f'{cli_arg}_defaults' in labdata_preferences.keys():
            return default # no defaults
        if labdata_preferences[f'{cli_arg}_defaults'] is None:
            return default # not defined dict
        if not argument in labdata_preferences[f'{cli_arg}_defaults'].keys():
            return default  # not defined
        return labdata_preferences[f'{cli_arg}_defaults'][argument]

def main():
    CLI_parser()
