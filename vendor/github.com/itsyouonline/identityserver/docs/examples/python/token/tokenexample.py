#!/usr/bin/env python3
import argparse
import requests


def getToken(application_id, secret, host, verify=True):
    print('Getting access token')
    r = requests.post('{}/v1/oauth/access_token?grant_type=client_credentials&client_id={}&client_secret={}'.format(host, application_id,secret), verify=verify)
    if r.status_code != 200:
        raise Exception('Response code {} - {}'.format(r.status_code, r.text))
    print(r.text)
    access_token = r.json()['access_token']
    username = r.json()['info']['username']
    return access_token, username

def main(application_id, secret, host, verify=True):
    access_token, username = getToken(application_id, secret, host, verify)

    print('Getting the user object using the token in the query parameters')
    r = requests.get('{}/api/users/{}?access_token={}'.format(host, username, access_token), verify=verify)
    print('{} - {}'.format(r.status_code,r.text))

    print('Getting the user object using the token in the Authorization header')
    r = requests.get('{}/api/users/{}'.format(host, username), headers={'Authorization':'token {}'.format(access_token)}, verify=verify)
    print('{} - {}'.format(r.status_code,r.text))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Sample application showing the client credentials flow')
    parser.add_argument('applicationid', help='Application ID of a user API Key')
    parser.add_argument('secret', help='Secret of a user API Key')
    parser.add_argument('--environment', default='https://itsyou.online')
    parser.add_argument('--no-verify', dest='noverify', action='store_true', help='Do not verify the ssl certificate')
    args = parser.parse_args()

    main(args.applicationid, args.secret, args.environment, verify=(not args.noverify))
