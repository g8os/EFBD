install_requirements(){
    apt update
    apt-get install -y curl git vim tmux ssh
    curl -s https://install.zerotier.com/ | bash
}

install_golang(){
    curl https://storage.googleapis.com/golang/go1.8.linux-amd64.tar.gz > go1.8.linux-amd64.tar.gz
    tar -C /usr/local -xzf go1.8.linux-amd64.tar.gz 
    export PATH=$PATH:/usr/local/go/bin && echo "export PATH=$PATH:/usr/local/go/bin" >> ~/.bash_profile
    mkdir /gopath 
    export GOPATH=/gopath && echo "export GOPATH=/gopath" >> ~/.bash_profile
    source ~/.bash_profile
}

install_etcd(){
    ETCD_VER=v3.2.5
    GITHUB_URL=https://github.com/coreos/etcd/releases/download 
    DOWNLOAD_URL=${GITHUB_URL}
    rm -f /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz 
    rm -rf /tmp/etcd-download-test && mkdir -p /tmp/etcd-download-test
    curl -L ${DOWNLOAD_URL}/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz -o /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz
    tar xzvf /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz -C /tmp/etcd-download-test --strip-components=1
}

install_zerostor(){
    go get -u github.com/zero-os/0-stor/server
}

echo "[*] Installing Requirements"
install_requirements
echo "[*] Installing GoLang"
install_golang
echo "[*] Installing ETCD server"
install_etcd
echo "[*] Installing ZeroStor server"
install_zerostor