action=$1
TEST_CASE=$2


install_client(){

    rm -rf /usr/local/go
    curl https://storage.googleapis.com/golang/go1.9.linux-amd64.tar.gz > go1.9.linux-amd64.tar.gz
    tar -C /usr/local -xzf go1.9.linux-amd64.tar.gz

    mkdir /gopath
    export GOPATH=/gopath
    export GOROOT=/usr/local/go
    export PATH=/usr/local/go/bin:$PATH

    echo "export GOPATH=/gopath" >> ~/.bash_profile
    echo "export GOROOT=/usr/local/go" >> ~/.bash_profile
    echo "export PATH=/usr/local/go/bin:$PATH" >> ~/.bash_profile

    source ~/.bash_profile
    go get -d github.com/zero-os/0-stor/client/cmd/zerostorcli
    cd /gopath/src/github.com/zero-os/0-stor/client/cmd/zerostorcli
    git checkout ${ZERO_STOR_CLIENT_BRANCH}
    echo " [*] Install zerostor client from branch : ${ZERO_STOR_CLIENT_BRANCH}"
    go build .
    chmod -R 777 /gopath
    ln -sf /gopath/src/github.com/zero-os/0-stor/client/cmd/zerostorcli/zerostorcli /bin/zerostorcli
    hash -r
}

join_zerotier_network(){
    sudo zerotier-cli join ${zerotier_network}; sleep 5
    member_id=$(sudo zerotier-cli info | awk '{print $3}')
    curl -H "Content-Type: application/json" \
    -H "Authorization: Bearer ${zerotier_token}" \
    -X POST -d '{"config": {"authorized": true}}' https://my.zerotier.com/api/network/${zerotier_network}/member/${member_id} >> /dev/null
}

if [ "$TRAVIS_EVENT_TYPE" == "cron" ] || [ "$TRAVIS_EVENT_TYPE" == "api" ]
    then
        if [ "$action" == "before" ]; then

            echo "[*] Create new zerotier network"
            zerotier_network=$(python3 test/prepare_testing_env/utils.py create_network)
            echo "[*] Zerotier Network ID : ${zerotier_network}"

            echo "[*] Join zerotier network"
            join_zerotier_network

            echo "[*] Installing client"
            travis_dir=$(pwd)
            sudo bash -c "$(declare -f install_client); install_client"

            cd ${travis_dir}
            echo '{}' > /tmp/config.json

            echo "[*] Configure ssh access"
            ssh-keygen -f $HOME/.ssh/id_rsa -t rsa -N ''
            python3 test/prepare_testing_env/utils.py config_ssh_access

            echo "[*] Create packet machine"
            python3 test/prepare_testing_env/utils.py create_device
            packet_machien_ip=$(cat /tmp/config.json | python -c 'import json, sys; print(json.load(sys.stdin)["device_ip"])')

            echo "[*] Create servers on packet machine"
            scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null test/prepare_testing_env/install_servers.sh test/prepare_testing_env/docker_script.sh root@${packet_machien_ip}:
            ssh -t -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null root@${packet_machien_ip} "bash install_servers.sh ${number_of_servers} ${zerotier_network} ${zerotier_token} ${ZERO_STORE_BRANCH}"
            servers_ips=$(ssh -t -oStrictHostKeyChecking=no root@${packet_machien_ip} cat servers_ips)
            echo "${servers_ips}"
            python3 test/prepare_testing_env/utils.py update_config "${servers_ips}" ${iyo_client_id} ${iyo_secret} ${iyo_organization} ${iyo_namespace}
            cat /gopath/src/github.com/zero-os/0-stor/client/cmd/zerostorcli/config.yaml

            echo "[*] Install test suite's requirements"
            pip3 install -r test/prepare_testing_env/requirements.txt

        elif [ "$action" == "test" ]; then

            echo " [*] Execute test case"
            cat /gopath/src/github.com/zero-os/0-stor/client/cmd/zerostorcli/config.yaml
            cd test && export PYTHONPATH='./' && nosetests-3.4 -vs $TEST_CASE --tc-file test_suite/config.ini --tc=main.number_of_servers:${number_of_servers} --tc=main.number_of_files:${number_of_files}

        elif [ "$action" == "after" ]; then
            python3 test/prepare_testing_env/utils.py teardown
        fi
    else
        echo "Not a cron job or trigger from api"
fi