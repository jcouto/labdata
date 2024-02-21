from .general import *
from .procedures import Watering
@dataschema
class DecisionTask(dj.Imported): # imported because if comes from data but there is no 'make'
    definition = '''
    -> Dataset
    ---
    n_total_trials                     : int     # number of trials in the session
    n_total_assisted = NULL            : int     # number of assisted trials in the session
    n_total_performed = NULL    : int     # number of self-performed trials
    n_total_initiated = NULL    : int     # number of initiated trials
    n_total_with_choice = NULL  : int     # number of self-initiated with choice
    n_total_rewarded = NULL     : int     # number of rewarded trials
    n_total_punished = NULL     : int     # number of punished trials
    -> [nullable] Watering                       # water intake during the session (ml) 
    '''
        
    class TrialSet(dj.Part):
        definition = '''
        -> master
        trialset_description     : varchar(54) # e.g. trial modality, unique condition
        ---
        n_trials                 : int         # total number of trials
        n_performed              : int         # number of self-performed trials
        n_with_choice            : int         # number of self-initiated trials with choice 
        n_correct                : int         # number of correct trials
        performance_easy         : float       # performance on easy trials
        performance              : float       # performance on all trials
        trial_num                : longblob    # trial number because TrialSets can be intertwined
        initiation_times = NULL  : longblob    # time between trial start and stim onset
        assisted = NULL          : longblob    # wether the trial was assisted
        response_values = NULL   : longblob    # left=1;no response=0; right=-1        
        correct_values = NULL    : longblob    # correct = 1; no_response  = NaN; wrong = 0        
        intensity_values = NULL  : longblob    # value of the stim (left-right)
        reaction_times = NULL    : longblob    # between onset of the response period and reporting  
        block_values = NULL      : longblob    # block number for each trial
        '''

        
