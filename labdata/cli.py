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

            run <analysis> -a <subject> -s <session>        Runs an analysis
            job <analysis> -a <subject> -s <session>        Runs an analysis as a job

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
 
            
    def _add_default_arguments(self, parser):
        parser.add_argument('-a','--subject',
                            action='store',
                            default=[''], type=str,nargs='+')
        parser.add_argument('-s','--session',
                            action='store',
                            default=[''], type=str,nargs='+')
        parser.add_argument('-d','--datatype',
                            action='store',
                            default=[''], type=str,nargs='+')
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
