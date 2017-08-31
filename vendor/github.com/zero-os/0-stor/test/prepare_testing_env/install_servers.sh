NUMBER_OF_SERVERS=${1:-1}
ZT_NT=$2
ZT_TOKEN=$3

install_requirements(){
    apt update
    apt install docker.io -y
    ssh-keygen -f $HOME/.ssh/id_rsa -t rsa -N ''
}

start_etcd_server(){
    docker exec -it ${1} bash -c "tmux new -d -s etcd /tmp/etcd-download-test/etcd \
    --advertise-client-urls  http://0.0.0.0:2379 \
    --listen-client-urls http://0.0.0.0:2379"
}

start_zerostor_server(){
    docker exec -it ${1} bash -c "tmux new -d -s zerostor /gopath/bin/server"
}

get_server_ip(){
    server_ip=$(docker exec -it ${1} bash -c "zerotier-cli listnetworks | grep ${ZT_NT}" | awk '{print $NF}' | awk -F / '{print $1}')
    echo ${server_ip}
}

join_zerotier_network(){
    SERVER_NAME=$1
    docker exec -it ${SERVER_NAME} bash -c "zerotier-cli join ${ZT_NT}"
    MEMBER_ID=$(docker exec ${SERVER_NAME} bash -c "zerotier-cli info" | awk '{print $3}')
    curl -H "Content-Type: application/json" \
    -H "Authorization: Bearer ${ZT_TOKEN}" \
    -X POST -d '{"config": {"authorized": true}}' https://my.zerotier.com/api/network/${ZT_NT}/member/${MEMBER_ID} >> /dev/null
}

#install requirements
install_requirements

# create basic image
docker create -it --name basic-image --device=/dev/net/tun --cap-add=NET_ADMIN ubuntu
docker start basic-image
docker cp docker_script.sh basic-image:/docker_script.sh
SSH_KEY=$(cat ~/.ssh/id_rsa.pub)
docker exec -it basic-image bash -c "mkdir ~/.ssh && echo ${SSH_KEY} >> ~/.ssh/authorized_keys"
docker exec -it basic-image bash -c "bash docker_script.sh"
docker commit basic-image zerostorserver-image

# create zerostor servers
SERVERS_ZT_IPS=()
for i in $(seq 1 ${NUMBER_OF_SERVERS})
do 
    SERVER_NAME="zerostorserver-${i}"
    SSH_PORT="400${i}"
    docker create -i -t --name ${SERVER_NAME} --device=/dev/net/tun --cap-add=NET_ADMIN -p ${SSH_PORT}:22 zerostorserver-image
    docker start ${SERVER_NAME}
    docker exec -it ${SERVER_NAME} bash -c "service ssh start"
    docker exec -d ${SERVER_NAME} bash -c "zerotier-one -d"; sleep 5
    join_zerotier_network ${SERVER_NAME}; sleep 5
    echo $(get_server_ip ${SERVER_NAME}) >> servers_ips
    start_etcd_server ${SERVER_NAME}
    start_zerostor_server ${SERVER_NAME}
done