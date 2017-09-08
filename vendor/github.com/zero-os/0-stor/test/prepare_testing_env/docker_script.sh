ZERO_STORE_BRANCH=$1

install_requirements(){
    apt update
    apt-get install -y curl git vim tmux ssh
    curl -s https://install.zerotier.com/ | bash
}

install_golang(){
    curl https://storage.googleapis.com/golang/go1.9.linux-amd64.tar.gz > go1.9.linux-amd64.tar.gz
    tar -C /usr/local -xzf go1.9.linux-amd64.tar.gz
    export PATH=$PATH:/usr/local/go/bin && echo "export PATH=$PATH:/usr/local/go/bin" >> ~/.bash_profile
    mkdir /gopath 
    export GOPATH=/gopath && echo "export GOPATH=/gopath" >> ~/.bash_profile
    source ~/.bash_profile
}

install_etcd(){
    ETCD_VER=v3.2.6
    GOOGLE_URL=https://storage.googleapis.com/etcd
    GITHUB_URL=https://github.com/coreos/etcd/releases/download
    DOWNLOAD_URL=${GOOGLE_URL}

    rm -f /home/etcd-${ETCD_VER}-linux-amd64.tar.gz
    rm -rf /home/etcd-download-test && mkdir -p /home/etcd-download-test

    curl -L ${DOWNLOAD_URL}/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz -o /home/etcd-${ETCD_VER}-linux-amd64.tar.gz
    tar xzvf /home/etcd-${ETCD_VER}-linux-amd64.tar.gz -C /home/etcd-download-test --strip-components=1

}

install_zerostor(){
    go get -d github.com/zero-os/0-stor/server
    cd /gopath/src/github.com/zero-os/0-stor/
    git checkout ${ZERO_STORE_BRANCH}
    cd server/
    go build
}

echo "[*] Installing Requirements"
install_requirements
echo "[*] Installing GoLang"
install_golang
echo "[*] Installing ETCD server"
install_etcd
echo "[*] Installing ZeroStor server"
install_zerostor