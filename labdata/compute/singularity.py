from ..utils import *
try:
    import spython
except:
    print("You'll need to install singularity-python:")
    print("     pip install spython")

def build_singularity_container(definition_file,
                                force = False,
                                output_folder = None):
    '''
    Builds a singularity container from a definition file.

    The default output folder is prefs['compute_containers']['local_path']

    e.g.
    image = build_singularity_container('/home/joao/lib/labdata/containers/labdata-base.sdef')

    Joao Couto - labdata 2024
    '''

    if output_folder is None:
        output_folder = prefs['compute_containers']['local_path']

    output_folder = Path(output_folder)
    if not output_folder.exists():
        output_folder.mkdir(parents=True,exist_ok = True)
        
    definition_file = Path(definition_file)
    container_file = (output_folder/definition_file.stem).with_suffix('.sif')
    
    from spython.main import Client

    image = Client.build(recipe = f'{definition_file}',  
                                 image = f'{container_file}', 
                                 stream = False, sudo = False, force = force, 
                                 options=["--fakeroot"])

    # alternatively this could also be done with the CLI 
    #from subprocess import check_output
    #check_output(f'singularity build --fakeroot {container_file} {definition_file}',shell= True)

    return image

def run_on_singularity(container, command, cuda = False):
    from spython.main import Client
    containerfile = Path(prefs['compute_containers']['local_path'])/'labdata-kilosort2_5.sif'
    if containerfile.exists():
        client = Client.execute(image = containerfile, command = 'nvidia-smi', nv=True)
    
