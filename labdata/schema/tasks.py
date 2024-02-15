from .general import *


@dataschema
class DecisionTask(dj.Imported): # imported because if comes from data but we wont actually have a 'make'
    definition = '''
    -> Dataset
    ---
    n_total_trials                     : int     # number of trials in the session
    n_total_assisted = NULL            : int     # number of assisted trials in the session
    n_total_trials_performed = NULL    : int     # number of self-performed trials
    n_total_trials_initiated = NULL    : int     # number of initiated trials
    n_total_trials_with_choice = NULL  : int     # number of trials with with choice
    n_total_trials_rewarded = NULL     : int     # number of rewarded trials
    n_total_trials_punished = NULL     : int     # number of punished trials
    -> [nullable] Watering                       # water intake during the session (ml) 
    '''
        
    class TrialSet(dj.Part):
        definition = '''
        -> Task
        trialset_description     : varchar(54)            # e.g. trial modality, unique condition
        ---
        n_trials                 : int                    # total number of trials
        n_performed              : int                    # number of performed trials
        n_correct                : int                    # number of correct trials
        reaction_times = NULL    : longblob               # time between onset of response period and report 
        initiation_times = NULL  : longblob               # time between trial start and stim onset 
        response_values = NULL   : longblob               # left=1;no response=0; right=-1        
        intensity_values = NULL  : longblob               # value of the stim (left-right)
        block_values = NULL      :            
        '''

