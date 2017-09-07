# 0-store test suite

## Test suite architecture:
```bash
├── prepare_testing_env
│   ├── docker_script.sh
│   ├── install_servers.sh
│   ├── requirements.txt
│   ├── run_tests.sh
│   └── utils.py
├── README.md
├── test_suite
│   ├── config.ini
│   ├── framework
│   │   ├── __init__.py
│   │   └── zero_store_cli.py
│   ├── __init__.py
│   ├── README.md
│   └── test_cases
│       ├── basic_tests
│       │   ├── __init__.py
│       │   ├── test01_upload_download.py
│       │   └── test02_pipeplining.py
│       ├── extend_test
│       │   └── __init__.py
│       └── testcases_base.py
└── test_suite.log
```

## Run Test suite:

You have to install an environment to run this test suite against it. You can isntall it manually or trigger it from the Travis dashboard. In case of using the Travis, It will automatically create an Ubuntu packet machine, create docker containers for each server and make sure that all those containers are joined a zero-tier network, then it will install a client in the Travis machine and execute the whole test cases against this environment.

If you have a local environment and you wanna execute this test suite against it, please follow these instructions.

- Manual steps:

```bash
cd /gopath/src/github.com/zero-os/0-stor/test
ln -sf /gopath/bin/zerostorcli /bin/zerostorcli
pip3 install -r prepare_testing_env/requirements.txt
export PYTHONPATH='./'
nosetests-3.4 -vs test_suite/test_cases --tc-file test_suite/config.ini
```
