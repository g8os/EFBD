# 0-stor daemon

## Goal
Giving the client the option to become a daemon with a e.g. grpc interface so that e.g. python can instruct the daemon to send/receive files.

We do this to prevent to have to re-implement all the client logic in every language we use.

### future feature
In a second phase we want to add a caching and autodiscovery feature to the deamon so it could be used as some kind of IPFS replacement.


## Requirements:
- The daemon should be started from the client CLI. We pass the same configuration as we would for a normal client.
- Simple interface, probably grpc. Grpc has a great number of supported language, that should give us the flexibilty we need.
- Commands:
  - SetObject(key, path)
  - GetObject(key, path)
