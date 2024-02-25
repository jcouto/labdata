from .utils import *
import argparse

class CLI_parser():
    def __init__(self):
        parser = argparse.ArgumentParser(
            description = 'labdata - tools to manage data in an experimental neuroscience lab',
            usage = ''' labdata <command> [args]
Data manipulation commands are:

            subjects                            list subjects
            sessions -a <subject>               list sessions 
            get -a <subject> -s <session>       download data from one session if not already there
            put -a <subject> -s <session>       copies a dataset to the server to be used
            clean                               deletes files that are already added

Data analysis commands:

            run <analysis> -a <subject> -s <session>        Runs an analysis, local, queued or on AWS
            job <analysis> -a <subject> -s <session>        Allocate an analysis, does not run

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
        parse_analysis(analysis = args.analysis,
                       job_id = args.job, # this will become un-used.
                       subject = args.subject,
                       session = args.session,
                       datatype = args.datatype,
                       secondary_args = secondary_args,
                       full_command = ' '.join(sys.argv[1:]))

    def task(self):
        parser = argparse.ArgumentParser(
            description = 'Runs a ComputeTask',
            usage = '''labdata task <JOB_ID> ''')
        parser.add_argument('job_id',action = 'store',default = None,type = int)
        args = parser.parse_args(sys.argv[2:])
        job_id = args.job_id
        from .compute import handle_job
        handle_job(job_id)
        
    def _add_default_arguments(self, parser):
        parser.add_argument('-a','--subject',
                            action='store',
                            default=None, type=str,nargs='+')
        parser.add_argument('-s','--session',
                            action='store',
                            default=None, type=str,nargs='+')
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
