from .general import *

@dataschema
class ProcedureType(dj.Lookup):
    definition = """
    procedure_type : varchar(52)       #  Defines procedures that are not an experimental session
    """
    contents = zip(['implant',
                    'injection',
                    'window replacement',
                    'handling',
                    'training',
                    'craniotomy'])
    
@dataschema
class Procedure(dj.Manual):
    ''' Surgical or behavioral manipulation. '''
    definition = """
    -> Subject
    -> ProcedureType
    procedure_datetime            : datetime
    ---
    -> LabMember
    procedure_metadata = NULL     : longblob   
    -> [nullable] Weighing
    -> [nullable] Note
    """

@dataschema
class Weighing(dj.Manual):
    definition = """
    -> Subject
    weighing_datetime : datetime
    ---
    weight : float  # (g)
    """

@dataschema
class Watering(dj.Manual):
    definition = """
    -> Subject
    watering_datetime : datetime
    ---
    water_volume : float  # (uL)
    """
    
@dataschema
class WaterRestriction(dj.Manual):
    definition = """
    -> Subject
    water_restriction_start_date : date
    ---
    -> LabMember
    water_restriction_end_date : date
   -> Weighing
    """

@dataschema
class Death(dj.Manual):
    definition = """
    -> Subject
    ---
    death_date:                  date       # death date
    death_ts=CURRENT_TIMESTAMP:  timestamp
    -> [nullable] Note
    """
