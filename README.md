## labdata

Tools to copy data and manage data analysis in an experimental neuroscience lab.

This is built after [labdata-tools](github.com/jcouto/labdata-tools) and integrates with S3 and a database through datajoint.

### Concepts
Experiments often involve many computers in the lab and data has to be copied to a server(and sometimes backed up). In this process files can be corrupted, data loss and so on, specially if manually copying files. Further, in some cases, tasks have to be performed on the files (those might involve compressing, formatting, etc).

What ``labdata`` does to copy files:
     1. After data are acquired, labdata performs a md5 checksum of the file to be copied and copies the file to a server.
     2. After the copy, the checksum and datapath are placed on the ``Upload`` database table.
     3. The computer/server that manages the copy to the permanent storage server will read the database table. The server will perform a checksum on the file to ensure the copy from the acquisition computer was ok.
     4. If needed it will compress data or do whatever is specified in the preference file rules (see ``upload_rules``)
     5. Data will be put in S3 and added to the ``Files``
     

### Instalation

Clone the repository to a folder in your computer and do:

``pip install .`` or ``pip install -e .`` to if you want that the source code follows git.
