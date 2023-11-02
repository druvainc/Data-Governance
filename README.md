# targeted_download
Download filtered webdav data using webdav APIs.
## Pre-requisites
 * Make sure that python3 or above is installed
 * modules to be installed
    *  python -m pip install requests
    

## Usage
```buildoutcfg
 python targeted_download_script.py --help
```
Sample comand to run the script
```buildoutcfg
python targeted_download.py 
--email="john.carter@druva.org" 
--password="password" 
--url="https://restore-c123-druva.com/webdav/policy1/user1/dev1/"
--ext_include=txt,py 
--name=filename 
--root_dir_name=folder123
--match_any_one=True
```


```buildoutcfg
------------------ | ----------------------------------------------
email*             | Legal admin email(username)
------------------ | ----------------------------------------------
password*          | Legal admin password.
------------------ | ----------------------------------------------
url*               | webdav url to download device data.
------------------ | ----------------------------------------------
ext_include        | Filter to include the extensions
------------------ | ----------------------------------------------
ext_exclude        | Filter to exclude the extensions
------------------ | ----------------------------------------------
name               | To filter the data by name of the file/folder
------------------ | ----------------------------------------------
root_dir_name      | To get the data from the root directory
------------------ | ----------------------------------------------
match_any_one      | True means OR operation and False means AND operation.
------------------ | ----------------------------------------------
list_folders_only  | To list only folders.
------------------ | ----------------------------------------------

```
Note: *indicates required argument.

## Creating email folder structure while downloading the eml files-
For email files(exchange online) In PROPFIND response while creating absolute file path use displayname property instead of href. 
e.g.
1. <D:href>/webdav/legal_hold_125/exchange/Exchange%20Online/Mails,v2971/F-QVFNa0FESXpOekkwWldJM0xURXlOelV0TkRabE1DMWlaVFExTFRoaU1ETTBNVFpqWmpCa1pRQXVBQUFEQ0U0aXdZOThDazYyNWdGZDljU01zUUVBRXRoYzFYb25qa1d1WWdkSkh1Z2FRZ0FBQWdFTUFBQUE%3D,v1/</D:href>
2. It has display name <D:displayname>Inbox,v1</D:displayname>.
3. so absolute file path for any email should contain "Inbox,v1" so that appropriate folder structure can be created.


## To Do -
To do in future-
1. Support for email folder structure while downloading the eml files.
2. Large file downloads using session management for consumer threads.
3. OS specific illegal characters to be handled for windows and mac OS.
4. Large file download using session management for consumer threads.
