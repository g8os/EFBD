action=$1

install_client(){

    rm -rf /usr/local/go
    curl https://storage.googleapis.com/golang/go1.8.linux-amd64.tar.gz > go1.8.linux-amd64.tar.gz
    tar -C /usr/local -xzf go1.8.linux-amd64.tar.gz 

    mkdir /gopath
    export GOPATH=/gopath
    export GOROOT=/usr/local/go
    export PATH=/usr/local/go/bin:$PATH

    echo "export GOPATH=/gopath" >> ~/.bash_profile
    echo "export GOROOT=/usr/local/go" >> ~/.bash_profile
    echo "export PATH=/usr/local/go/bin:$PATH" >> ~/.bash_profile

    source ~/.bash_profile
    go get -u github.com/zero-os/0-stor/client/cmd/zerostorcli
    chmod -R 777 /gopath
    ln -sf /gopath/bin/zerostorcli /bin/zerostorcli
    hash -r
}

if [ "$action" == "before" ]; then

    echo "[*] Installing client"
    travis_dir=$(pwd)
    sudo bash -c "$(declare -f install_client); install_client"

    cd ${travis_dir}

    echo "[*] Configure ssh access"
    ssh-keygen -f $HOME/.ssh/id_rsa -t rsa -N ''
    python3 test/prepare_testiing_env/utils.py config_ssh_access ${packet_token}
    echo '{}' > /tmp/config.json

    echo "[*] Create packet machine"   
    python3 test/prepare_testiing_env/utils.py create_device ${packet_token}
    packet_machien_ip=$(cat /tmp/config.json | python -c 'import json, sys; print(json.load(sys.stdin)["device_ip"])')
    
    echo "[*] Create servers on packet machine"  
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null test/prepare_testiing_env/install_servers.sh test/prepare_testiing_env/docker_script.sh root@${packet_machien_ip}:
    ssh -t -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null root@${packet_machien_ip} "bash install_servers.sh ${number_of_servers} ${zerotier_network} ${zerotier_token}"
    servers_ips=$(ssh -t -oStrictHostKeyChecking=no root@${packet_machien_ip} cat servers_ips)
    echo "${servers_ips}"
    python3 test/prepare_testiing_env/utils.py update_config "${servers_ips}"
    cat /gopath/src/github.com/zero-os/0-stor/client/cmd/zerostorcli/config.yaml

elif [ "$action" == "test" ]; then   

    echo "Test client"
    zerostorcli

elif [ "$action" == "after" ]; then

    python3 test/prepare_testiing_env/utils.py delete_device ${packet_token}

fi