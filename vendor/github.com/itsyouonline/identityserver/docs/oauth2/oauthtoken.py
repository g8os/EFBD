#!/usr/bin/env python
import argparse
import subprocess
import os
import requests
from ConfigParser import ConfigParser

def get_oauth_token(configdata):
    authorizeurl = '{url}//v1/oauth/authorize?response_type=code&client_id={clientid}&redirect_uri={redirect_url}&scope={scope}&state=STATE'.format(**configdata)
    devnull = open(os.devnull)
    subprocess.Popen(['xdg-open', authorizeurl], stdout=devnull, stderr=devnull)
    code = raw_input("Enter returned code: ")
    configdata['code'] = code
    tokenurl =  "{url}/v1/oauth/access_token?client_id={clientid}&client_secret={secret}&code={code}&redirect_uri={redirect_url}&state=STATE".format(**configdata)

    response = requests.post(tokenurl, verify=False)
    result = response.json()
    print(result)

def get_jwt_token(configdata, oauthtoken):
    session = requests.Session()
    session.headers.update({'Authorization': 'token %s' % oauthtoken})
    jwturl = '{url}/v1/oauth/jwt?scope=user:memberOf:{clientid}:{scope}'.format(**configdata)
    response = session.post(jwturl, verify=False)
    result = response.text
    print(result)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['jwt', 'oauth'])
    parser.add_argument('-t', '--token')
    parser.add_argument('-s', '--section', default='default')
    options = parser.parse_args()
    config = ConfigParser()
    config.read('config.ini')
    configdata = dict(config.items(options.section))

    if options.action == 'oauth':
        get_oauth_token(configdata)
    elif options.action == 'jwt':
        get_jwt_token(configdata, options.token)
