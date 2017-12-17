# boto_scripts

## glacier_backup.py

This tool backs up local directories to Amazon glacier

### Installation

        mkdir ~/glacier_backups
        pipenv install Pipfile


### Usage

        python glacier_backup.py --vault_name backups --directory my_dir
        
If your directory contains sub directories, these will be uploaded as distinct glacier archives. 

As such, this tool is best suited to backing up media or documents.

### Retieval

By default, glacier_backup.py stores metadata about the archives here: /tmp/glacier_metadata.json
        
This file has the following structure
        
            {
            "some_directory": {
                "archive_id": "....", 
                "sha256sum": "...."
                }
            }

The archive_id can be used to obtain the archive for the directory "some_directory" from glacier.

The sha256 sum is used upon subsequent backups, to ensure directories are not backed up redundantly.

 