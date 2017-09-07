import packet, yaml, sys, random, json, time, os, requests

def create_zerotier_network():
    zt_token = os.environ['zerotier_token']
    session = requests.Session()
    session.headers['Authorization'] = 'Bearer %s' % zt_token
    url = 'https://my.zerotier.com/api/network'
    data = {'config': {'ipAssignmentPools': [{'ipRangeEnd': '10.147.17.254',
                                              'ipRangeStart': '10.147.17.1'}],
                       'private': 'true',
                       'name': environment_id,
                       'routes': [{'target': '10.147.17.0/24', 'via': None}],
                       'v4AssignMode': {'zt': 'true'}}}

    response = session.post(url=url, json=data)
    zerotier_network_id = response.json()['id']
    return zerotier_network_id

def add_public_key():
    ssh_path = os.path.expanduser('~') + '/.ssh/id_rsa.pub'
    with open(ssh_path, 'r') as f:
        public_key = f.read()

    label = environment_id
    ssh_key = packet_manager.create_ssh_key(label=label, public_key=public_key)
    save_config(ssh_key_id=ssh_key.id)

def create_new_device(hostname, plan, operating_system, facility):
    project = packet_manager.list_projects()[0]
    device = packet_manager.create_device(project_id=project.id,
                                          hostname=hostname,
                                          plan=plan,
                                          operating_system=operating_system,
                                          facility=facility)
    return device

def get_available_facility(plan):
    facilities = [x.code for x in packet_manager.list_facilities()]
    for facility in facilities:
        if packet_manager.validate_capacity([(facility, plan, 1)]):
            return facility
    else:
        return None

def delete_public_key(ssh_key_id):
    packet_manager.call_api('ssh-keys/%s' % ssh_key_id, type='DELETE')

def delete_devices(device_id):
    packet_manager.call_api('devices/%s' % device_id, type='DELETE')
    
def update_config_file(servers_ips, iyo_client_id, iyo_secret, iyo_org, iyo_namespace):
    config_path = '/gopath/src/github.com/zero-os/0-stor/client/cmd/zerostorcli/config.yaml'
    with open(config_path, 'r') as f:
        config = yaml.load(f)

    config['shards'] = ['http://{}:8080'.format(x) for x in servers_ips]
    config['meta_shards'] = ['http://{}:2379'.format(x) for x in servers_ips]
    config['iyo_app_id'] = iyo_client_id
    config['iyo_app_secret'] = iyo_secret
    config['organization'] = iyo_org
    config['namespace'] = iyo_namespace
    config['protocol'] = 'rest'

    with open(config_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)


def read_config():
    with open('/tmp/config.json', 'r') as f:
        return json.load(f)


def save_config(**kwargs):
    config = read_config()
    for key, value in kwargs.items():
        config[key] = value

    with open('/tmp/config.json', 'w') as f:
        json.dump(config, f)


if __name__ == '__main__':
    action = sys.argv[1]
    environment_id = 'travis-zerostor-{}'.format(os.environ['TRAVIS_BUILD_NUMBER'])
    packet_manager = packet.Manager(auth_token=os.environ['packet_token'])
    
    if action == 'create_network':
        zerotier_network_id = create_zerotier_network()
        print(zerotier_network_id)

    elif action == 'create_device':
        hostname = environment_id
        plan = 'baremetal_0'
        operating_system = 'ubuntu_16_04'
        facility = get_available_facility(plan)

        if not facility:
            raise RuntimeError('No available facillity was found')

        device = create_new_device(hostname=hostname,
                                   plan=plan,
                                   operating_system=operating_system,
                                   facility=facility)

        save_config(device_id=device.id)

        for i in range(20):
            _device = packet_manager.get_device(device.id)
            if _device.state == 'active':
                break
            else:
                'Watting packet machine to be active ...'
                time.sleep(30)
        else:
            raise RuntimeError('Timeout when creating packet machine')

        device_ip = _device.ip_addresses[0]['address']
        save_config(device_ip=device_ip)


    elif action == 'config_ssh_access':
        add_public_key()

    elif action == 'update_config':
        servers_ips = sys.argv[2].splitlines()
        iyo_client_id = sys.argv[3]
        iyo_client_secret = sys.argv[4]
        iyo_organization = sys.argv[5]
        iyo_namespace = sys.argv[6]
        update_config_file(servers_ips, iyo_client_id, iyo_client_secret, iyo_organization, iyo_namespace)

    elif action == 'teardown':
        config = read_config()
        delete_devices(config['device_id'])
        delete_public_key(config['ssh_key_id'])