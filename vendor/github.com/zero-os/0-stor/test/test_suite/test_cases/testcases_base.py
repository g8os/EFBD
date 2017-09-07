import uuid, random, time, signal, logging, yaml, queue, json
from unittest import TestCase
from nose.tools import TimeExpired
from termcolor import colored
from test_suite.framework.zero_store_cli import ZeroStoreCLI
import threading
from hashlib import md5
import subprocess
from testconfig import config
import os


class TestcasesBase(TestCase):
    created_files_info = {}
    uploaded_files_info = {}
    downloaded_files_info = {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.utiles = Utiles()
        self.lg = self.utiles.logging

        self.zero_store_cli = ZeroStoreCLI()
        gopath = os.environ.get('GOPATH', '/gopath')
        self.default_config_path = '{gopath}/src/github.com/zero-os/0-stor/client/cmd/zerostorcli/config.yaml'.format(gopath=gopath)

        self.number_of_servers = int(config['main']['number_of_servers'])
        self.number_of_files = int(config['main']['number_of_files'])

        self.uploader_job_ids = []
        self.downloader_job_ids = []

        self.writer_jobs = queue.Queue()
        self.reader_jobs = queue.Queue()

        self.upload_queue_result = queue.Queue()
        self.download_queue_result = queue.Queue()

    @classmethod
    def setUpClass(cls):
        self = cls()
        self.create_files(self.number_of_files)

    def setUp(self):
        self._testID = self._testMethodName
        self._startTime = time.time()

        def timeout_handler(signum, frame):
            raise TimeExpired('Timeout expired before end of test %s' % self._testID)

        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(600)

    def create_files(self, number_of_files):
        self.execute_shell_commands(cmd='rm -rf /tmp/upload; rm -rf /tmp/download')
        self.execute_shell_commands(cmd='mkdir -p /tmp/upload; mkdir -p /tmp/download')
        for i in range(0, number_of_files):
            file_size = random.randint(1024, 1024 * 1024)
            file_path = '/tmp/upload/upload_file_%d' % random.randint(1, 1000)

            with open(file_path, 'w') as file:
                file.write(str(random.randint(0, 9)) * file_size)
            file_md5 = self.md5sum(file_path=file_path)
            TestcasesBase.created_files_info['id_%d' % i] = {'path': file_path,
                                                             'size': file_size,
                                                             'md5': file_md5}
            print(colored(' [*] Create : %s size %s , md5 %s ' % (file_path, str(file_size), str(file_md5)), 'white'))

    def md5sum(self, file_path):
        hash = md5()
        with open(file_path, "rb") as file:
            for chunk in iter(lambda: file.read(128 * hash.block_size), b""):
                hash.update(chunk)
        return hash.hexdigest()

    def create_new_config_file(self, new_config):
        """
            Logic:
                - It takes new config dict, read the default config file as dict, update the default config file dict,
                create a new config file with the updated dict.
        """
        with open(self.default_config_path, 'r') as stream:
            config = yaml.load(stream)
        config.update(new_config)
        new_config_file_path = '/tmp/config_%s.yaml' % self.random_string()
        with open(new_config_file_path, 'w') as new_config_file:
            yaml.dump(config, new_config_file, default_flow_style=False)
        return new_config_file_path

    def get_piplining_compination(self):
        data, parity = self.get_data_parity()
        pipes_options = []
        for chunk in ['0', 'chunk']:
            for compress in ['0', 'snappy', 'lz', 'gzip']:
                for encrypt in ['0', 'sha256', 'blake2', 'md5']:
                    for distribution in ['0', 'distribution']:
                        pipes_options.append({'chunk': chunk,
                                              'compress': compress,
                                              'encrypt': encrypt,
                                              'distribution': distribution})
        pipes_compination = []
        for option in pipes_options:
            data = {'pipes': []}
            if option['chunk'] != '0':
                chunker = {'name': self.random_string(),
                           'config': {'chunkSize': random.randint(1, 1024000000)},
                           'type': 'chunker'}
                data['pipes'].append(chunker)

            if option['compress'] != '0':
                compress = {'name': self.random_string(),
                            'config': {'type': option['compress']},
                            'type': 'compress'}
                data['pipes'].append(compress)

            if option['encrypt'] != '0':
                encrypt = {'name': self.random_string(),
                           'config': {'nonce': 123456789012,
                                      'privKey': 'ab345678901234567890123456789012',
                                      'type': option['encrypt']},
                           'type': 'encrypt'}
                data['pipes'].append(encrypt)

            if option['distribution'] != '0':
                distribution = {'name': self.random_string(),
                                'config': {'data': data, 'parity': parity},
                                'type': 'distribution'}
                data['pipes'].append(distribution)
            pipes_compination.append(data)
        return pipes_compination

    def get_data_parity(self):
        while True:
            data = random.randint(1, self.number_of_servers)
            parity = random.randint(1, self.number_of_servers)
            if data + parity == self.number_of_servers:
                print(colored(' [*] data: %d, parity: %d' % (data, parity), 'white'))
                break
        return data, parity

    def random_string(self, size=10):
        return str(uuid.uuid4()).replace('-', '')[:size]

    def default_writer(self):
        return self.writer(created_files_info=TestcasesBase.created_files_info, config_list=[self.default_config_path])

    def writer(self, created_files_info, config_list, number_of_threads=1):
        """
            This writer takes:
                - dict of create_files {id_xx:{'path':'x', 'size':'xx', 'md5':'xx', 'thread':''} .. }
                - list of config, each item represent a dict of config
                - number of threads
            Logic:
                - If its only one file and its only one config, you can send any number of threads.
                else, it will create threads equal to #files * #configs and each thread will send each file
                with different config.
                - It will update the updated_files_info by adding the thread to each file
                - It will return threads too
        """
        if len(created_files_info) == 1 and len(config_list) == 1 and number_of_threads != 1:
            key = list(created_files_info.keys())[0]
            for i in range(0, number_of_threads):
                job_id = self.create_job_id(job_ids_list=self.uploader_job_ids)
                self.create_writer_jobs(job_id=job_id, file_info=created_files_info[key],
                                        config_path=config_list[0])
        else:
            number_of_threads = len(created_files_info) * len(config_list)
            for file_key in created_files_info:
                for config in config_list:
                    job_id = self.create_job_id(job_ids_list=self.uploader_job_ids)
                    self.uploader_job_ids.append(job_id)
                    self.create_writer_jobs(job_id=job_id, file_info=created_files_info[file_key],
                                            config_path=config)

        jobs_queue = list(self.writer_jobs.queue)
        print(colored(' [*] Number of writer threads : %i' % number_of_threads, 'white'))
        for i in range(0, number_of_threads):
            thread = threading.Thread(target=self.writer_work)
            thread.start()
            TestcasesBase.uploaded_files_info[jobs_queue[i]['id']]['thread'] = thread
        return TestcasesBase.uploaded_files_info

    def writer_work(self):
        while not self.writer_jobs.empty():
            job = self.writer_jobs.get()
            self.zero_store_cli.upload_file(job=job, queue=self.upload_queue_result)
            self.writer_jobs.task_done()

    def create_writer_jobs(self, job_id, file_info, config_path):
        self.writer_jobs.put({'id': job_id,
                              'file_path': file_info['path'],
                              'config_path': config_path})
        TestcasesBase.uploaded_files_info.update({job_id: {'path': file_info['path'],
                                                           'config_path': config_path,
                                                           'size': file_info['size'],
                                                           'md5': file_info['md5']}})

    def create_job_id(self, job_ids_list):
        while True:
            job_id = random.getrandbits(128)
            if job_id not in job_ids_list:
                return job_id

    def get_uploaded_files_keys(self):
        """
            This method block into threads and update uploaded_file_info with key
        """
        for job_id in TestcasesBase.uploaded_files_info:
            thread = TestcasesBase.uploaded_files_info[job_id]['thread']
            thread.join()
            upload_queue_list = list(self.upload_queue_result.queue)
            for data in upload_queue_list:
                if data['job_id'] == job_id:
                    TestcasesBase.uploaded_files_info[job_id]['key'] = data['uploaded_key']
                    break
            else:
                TestcasesBase.uploaded_files_info[job_id]['key'] = 'ERROR! : upload_queue does not have this key'

    def default_reader(self):
        return self.reader(uploaded_files_info=TestcasesBase.uploaded_files_info)

    def reader(self, uploaded_files_info, number_of_threads=1):
        """
            uploaded_files_info = {job_id: {file_paht:'', file_config:'', file_size:'', md5:'', key:''} }
        """
        if number_of_threads < len(uploaded_files_info):
            number_of_threads = len(uploaded_files_info)

        if number_of_threads == len(uploaded_files_info):
            for uploader_job_id in uploaded_files_info:
                job_id = self.create_job_id(job_ids_list=self.downloader_job_ids)

                self.create_reader_jobs(job_id=job_id, uploader_job_id=uploader_job_id,
                                        uploader_file_info=uploaded_files_info[uploader_job_id])
        else:
            for i in range(0, number_of_threads):
                job_id = self.create_job_id(job_ids_list=self.downloader_job_ids)
                uploader_job_id = random.choice(self.uploader_job_ids)
                self.create_reader_jobs(job_id=job_id, uploader_job_id=uploader_job_id,
                                        uploader_file_info=uploaded_files_info[uploader_job_id])

        jobs_queue = list(self.reader_jobs.queue)
        for i in range(0, number_of_threads):
            thread = threading.Thread(target=self.reader_work)
            thread.start()
            TestcasesBase.downloaded_files_info[jobs_queue[i]['id']]['d_info']['thread'] = thread
        return TestcasesBase.downloaded_files_info

    def reader_work(self):
        while not self.reader_jobs.empty():
            job = self.reader_jobs.get()
            self.zero_store_cli.download_file(job=job, queue=self.download_queue_result)
            self.reader_jobs.task_done()

    def create_reader_jobs(self, job_id, uploader_job_id, uploader_file_info):
        self.reader_jobs.put({'id': job_id,
                              'key': uploader_file_info['key'],
                              'config_path': uploader_file_info['config_path'],
                              'result': '/tmp/download/' + str(job_id)})
        TestcasesBase.downloaded_files_info.update({job_id: {'u_info': {'uploaded_job_id': uploader_job_id,
                                                                        'key': uploader_file_info['key'],
                                                                        'md5': uploader_file_info['md5'],
                                                                        'size': uploader_file_info['size']},
                                                             'd_info': {}
                                                             }
                                                    })

    def get_download_files_paths_from_threads(self):
        """
            This method block into threads and update downloaded_files_info with result
        """
        for job_id in TestcasesBase.downloaded_files_info:
            thread = TestcasesBase.downloaded_files_info[job_id]['d_info']['thread']
            thread.join()
            dowhload_queue_list = list(self.download_queue_result.queue)
            for data in dowhload_queue_list:
                if data['job_id'] == job_id:
                    if "ERROR" in data['downloaded_path']:
                        TestcasesBase.downloaded_files_info[job_id]['d_info']['path'] = data['downloaded_path']
                        TestcasesBase.downloaded_files_info[job_id]['d_info']['md5'] = '000000000000000000000000000000'
                    else:
                        TestcasesBase.downloaded_files_info[job_id]['d_info']['path'] = data['downloaded_path']
                        TestcasesBase.downloaded_files_info[job_id]['d_info']['md5'] = self.md5sum(
                            file_path=data['downloaded_path'])
                    break
            else:
                TestcasesBase.downloaded_files_info[job_id]['d_info'][
                    'path'] = 'ERROR! : download queue does not have this key'

    def check_md5(self, downloaded_files_info):
        print(colored(' [*] Compare downloaded files with uploaded files.', 'white'))
        corrupted_files = {}
        for id in downloaded_files_info:
            if downloaded_files_info[id]['d_info']['md5'] != downloaded_files_info[id]['u_info']['md5']:
                corrupted_files[id] = {'d_info': {'path': downloaded_files_info[id]['d_info']['path'],
                                                  'md5': downloaded_files_info[id]['d_info']['md5']
                                                  },
                                       'u_info': {'key': downloaded_files_info[id]['u_info']['key'],
                                                  'md5': downloaded_files_info[id]['u_info']['md5']
                                                  }
                                       }
        return corrupted_files

    def get_random_file_to_upload(self):
        file_id = 'id_%d' % random.randint(1, 10)
        return TestcasesBase.created_files_info[file_id]

    def execute_shell_commands(self, cmd):
        # print(colored(" [*] Execute: %s" % cmd, 'white'))
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, error = process.communicate()
        # if error:
        #     print(colored(' [*] Error!! %s' % error.decode('utf-8'), 'red'))
        # else:
        #     print(colored(" [*] OK.", 'green'))
        return out.decode('utf-8'), error.decode('utf-8')


class Utiles:
    def __init__(self):
        self.config = {}
        self.logging = logging
        self.log('test_suite.log')

    def log(self, log_file_name='log.log'):
        log = self.logging.getLogger()
        fileHandler = self.logging.FileHandler(log_file_name)
        log.addHandler(fileHandler)
        self.logging.basicConfig(filename=log_file_name, filemode='rw', level=logging.INFO,
                                 format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    def print_pretty_dict(self, dict, color):
        print('\n')
        print(colored(json.dumps(dict, sort_keys=True, indent=4), color))
