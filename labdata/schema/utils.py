from ..utils import *
# some functions used in the schema imports

__all__ = ['read_events_from_btss_riglog']

def read_events_from_btss_riglog(logfile):
    '''
    Read events from BTSS (Behavior Training and Sensory Stimulation) riglog files.
    Parses the log files and prepares the events to be inserted to DatasetEvents.

    eventsdict = read_events_from_btss_riglog(logfile)

    Joao Couto - labdata 2024
    '''
    from btss import parse_riglog   # github.com/jcouto/btss
    
    log,comm = parse_riglog(str(logfile))
    # insert btss log 
    datasetevents = []

    for k in log.keys():
        ev = log[k]
        if not ev is None:
            if not k in ['vstim']: # log the teensy/arduino
                stream_name = 'duino'
                t = np.array(ev['duinotime'].values,float)
                v = ev['value'].values
                try:
                    v = np.array(v,dtype = float)
                except: # let it be
                    pass
                idx = np.argsort(t) # make sure these are sorted
                datasetevents.append(dict(stream_name = stream_name,
                                          event_name = k,
                                          event_timestamps = t[idx],  # in seconds
                                          event_values = v[idx]))
            elif k == 'vstim': # log psychopy
                stream_name = k
                for d in ev.columns:
                    if not d in ['code','timereceived']:
                        t = ev['timereceived'].values
                        v = ev[d].values
                        idx = np.arange(len(v))
                        # if np.isreal(v[0]):
                        #     idx = np.isfinite(v)  # select only finite
                        t = np.array(t[idx],dtype = float)
                        try: # cast ints
                            v = np.array(v[idx],dtype = float)
                        except: # let it be
                            v = v[idx]
                        idx = np.argsort(t)
                        datasetevents.append(dict(stream_name = stream_name,
                                              event_name = d,
                                              event_timestamps = t[idx],  # in seconds
                                              event_values = v[idx]))
    # parse the comments for the temperature sensor readings
    stream_name = 'duino'
    to_parse = dict(temperature = [],pressure = [],humidity = [])
    time = []
    import re
    for c in comm:
        for i,k in enumerate(to_parse.keys()):
            if k in c:
                tmp = [float(f) for f in re.findall("\d+\.\d+",c)]
                if len(tmp) == 2:
                    t,v = tmp
                    if i == 0:
                        time.append(t)
                    to_parse[k].append(v)
                else:
                    print(f'Error in {c} for {logfile}')
    if len(time):
        for k in to_parse.keys():
            datasetevents.append(dict(stream_name = stream_name,
                                  event_name = k,
                                  event_timestamps = time,  # in seconds
                                  event_values = to_parse[k]))
    return datasetevents
