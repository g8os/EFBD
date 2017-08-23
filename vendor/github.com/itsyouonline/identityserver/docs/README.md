# Itsyou.online

## Clients

### GO

In your code, import the client from itsyou.online:

```
import "github.com/itsyouonline/identityserver/clients/go/itsyouonline"
```

### Python

There is no pypi published python client yet but it is available at https://github.com/itsyouonline/identityserver/tree/master/clients/python/itsyouonline

Example:

Get a client_id and client_secret as described in the [client credentials oauth flow](oauth2/oauth2.md#user-api-key).

```
client = itsyouonline.Client()
applicationid = 'YYYYYY'
secret = 'XXXXXXXXX'
client.oauth.LoginViaClientCredentials(client_id=applicationid, client_secret=secret)

client.api.GetUserPhonenumberByLabel('Home', 'John').json()
```

If you have a normal oauth token, you can set it on the client as well:
```
client.api.session.headers['Authorization'] = 'token <oauth token>'
```

Or when you have a jwt:
```
client.api.session.headers['Authorization'] = 'bearer <jwt>'
```

### Jumpscale

The itsyou.online client is preregistered in Jumpscale: https://github.com/Jumpscale/jumpscale_core8/blob/master/lib/JumpScale/clients/itsyouonline/readme.md
