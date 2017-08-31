import packet, yaml, sys, random, json, time, os

def add_public_key():
    ssh_path = os.path.expanduser('~') + '/.ssh/id_rsa.pub'
    with open(ssh_path, 'r') as f:
        public_key = f.read()

    label = environment_id
    packet_manager.create_ssh_key(label=label, public_key=public_key)

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


def delete_devices(device_id):
    packet_manager.call_api('devices/%s' % device_id, type='DELETE')


def update_config_file(servers_ips):
    config_path = '/gopath/src/github.com/zero-os/0-stor/client/cmd/zerostorcli/config.yaml'
    with open(config_path, 'r') as f:
        config = yaml.load(f)
    
    config['shards'] = ['http://{}:8080'.format(x) for x in servers_ips]
    config['meta_shards'] = ['http://{}:2379'.format(x) for x in servers_ips]

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
    environment_id = 'travis-zerostor-{}'.format(random.randint(0, 1000))
    if action == 'create_device':
        token = sys.argv[2]
        packet_manager = packet.Manager(auth_token=token)
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
        
        device_ip =  _device.ip_addresses[0]['address']
        save_config(device_ip=device_ip)

    elif action == 'delete_device':
        token = sys.argv[2]
        packet_manager = packet.Manager(auth_token=token)
        device_id = read_config()['device_id']
        delete_devices(device_id)

    elif action == 'config_ssh_access':
        token = sys.argv[2]
        packet_manager = packet.Manager(auth_token=token)
        add_public_key()

    elif action == 'update_config':
        servers_ips = sys.argv[2].splitlines()
        update_config_file(servers_ips)