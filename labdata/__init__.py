VERSION = "0.0.1"

from .utils import *
from .copy import copy_to_upload_server
from .s3 import copy_to_s3


plugins = {}  # to have all plugins in the same place
if 'plugins' in prefs.keys():
    for modkey in prefs['plugins'].keys():
        try:
            modpath = str(Path(prefs['plugins'][modkey])/"__init__.py")
            exec(f"{modkey} = plugin_lazy_import('{modkey}')")
            exec(f'plugins["{modkey}"] = {modkey}')
        except Exception as err:
            print(err)
            print(f'Failed to load plugin: {modkey}')
