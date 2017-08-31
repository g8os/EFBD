from termcolor import colored
import subprocess


class ZeroStoreCLI():
    def download_file(self, job, queue):
        job_id = job['id']
        key = job['key']
        config_path = job['config_path']
        result = job['result']
        command = 'zerostorcli --conf %s file download %s %s' % (config_path, key, result)
        #command = 'cd /gopath/src/github.com/zero-os/0-stor/client/cmd/zerostorcli/ && zerostorcli file download %s %s' % (key, result)
        out, error = self.execute_shell_commands(command)
        if error:
            queue.put({'job_id': job_id,
                       'downloaded_path': 'ERROR! %s ' % error})
        else:
            result = out.split('\n')[-2]
            if 'file downloaded' in result:
                path = result.split(' ')[-1]
                queue.put({'job_id': job_id,
                           'downloaded_path': path})
            else:
                queue.put({'job_id': job_id,
                           'downloaded_path': 'ERROR! %s ' % str(out)})

    def upload_file(self, job, queue):
        file_path = job['file_path']
        config_path = job['config_path']
        job_id = job['id']
        command = 'zerostorcli --conf %s file upload %s' % (config_path, file_path)
        #command = 'cd /gopath/src/github.com/zero-os/0-stor/client/cmd/zerostorcli/ && zerostorcli file upload %s' % file_path
        out, error = self.execute_shell_commands(command)
        if error:
            queue.put({'job_id': job_id,
                       'downloaded_path': 'ERROR! %s ' % error})
        else:
            result = out.split('\n')[-2]
            if 'file uploaded' in result:
                key = result.split(' ')[-1]
                queue.put({'job_id': job_id,
                           'uploaded_key': key})
            else:
                queue.put({'job_id': job_id,
                           'uploaded_key': 'ERROR!'})

    def create_namespace(self, namespace):
        command = 'zerostorcli namespace create %s' % namespace
        self.execute_shell_commands(command)

    def delete_namespace(self, namespace):
        command = 'zerostorcli namespace delete %s' % namespace
        self.execute_shell_commands(command)

    def get_user_acl(self, namespace, username):
        command = "zerostorcli namespace get-acl --namespace %s --user %s" % (namespace, username)
        self.execute_shell_commands(command)

    def set_user_acdl(self, namespace, username, permission_list):
        data = []
        permissions = ''
        for permission in permission_list:
            data.append('-', permission[0])
        for permission in data:
            permissions += permission + ' '
        permissions = permissions[:-1]
        command = "zerostorcli namespace set-acl --namespace %s --user %s %s" % (namespace, username, permissions)
        self.execute_shell_commands(command)

    def execute_shell_commands(self, cmd):
        #print(colored(" [*] Execute: %s" % cmd, 'white'))
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, error = process.communicate()
        # if error:
        #     print(colored(' [*] Error!! %s' % error.decode('utf-8'), 'red'))
        # else:
        #     print(colored(" [*] OK.", 'green'))
        return out.decode('utf-8'), error.decode('utf-8')
