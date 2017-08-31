# Test suite logic: ##

# Mean methods description:
**def create_files(self, number_of_files):**
- It creates 10 files with random size and return the following dict info.
```python
created_files_info = {'id_001': {
    'path': <'file path'>,
    'size': <'file size'>,
    'md5' : <'md5 checksum'>
}} 
```
**def writer(self, created_files_info, config_list, number_of_threads=1)**
- writer methods creates multithreads to upload files, it takes the created_files_info dict, creates job id for every job, creates thread for every job, those threads will run to upload files. This method returns updated_files_info dict

```python
updated_files_info = {'unique id <job_id>': {
    'path': <'file path'>,
    'config_path': <'config_path'>,
    'size': <'file size'>,
    'md5' : <'md5 checksum'>,
    'thread' : <thread>
}} 
```

**def upload_file(self, job, queue)**

This method is the main uploading method. It takes job dict
 ```python
  {'jobid':<job_id>,
   'file_path': <file_path>,
   'config_path': <config_path>}
```
and it updates the upload_queue with 
```python
{'job_id': <job_id>,
 'uploade_key': <key>}
```

**def get_uploaded_files_keys(self)**

This method is looping over all uploading threads to get the uploaded key. Then it updates the uploaded_file_info dict with the uploaded key values.

```python
updated_files_info = {'unique id <job_id>': {
    'path': <'file path'>,
    'config_path': <'config_path'>,
    'size': <'file size'>,
    'md5' : <'md5 checksum'>,
    'thread' : <thread>,
    'key': <key_value>
}} 
```

**def reader()**

This method creates multi threads to download file form the remote server. It takes the uploaded file dict, gnerates unique job_id, start threads, updates downloaded_files_info dict.

```python
downdloaded_files_info = {job_id: 
{'u_info': {'uploaded_job_id': uploader_job_id,
            'key': uploader_job_id['key'],
            'md5': uploader_job_id['md5'],
            'size': uploader_job_id['size']}},

'd_info': {'thread': <Downloader thread>}}
```

**def get_piplining_compination(self):**
It returns list of all possible compination for pipes.

