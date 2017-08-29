# JWT (JSON Web Token) support

Even though the OAuth token support works great for applications that need to access the information of a user, when passing on some of these authorizations to a third party service it is not a good idea to pass on your token itself.

The token you acquired might give access to a lot more information that you want to pass on to the third party service and it is required to invoke itsyou.online to verify that the authorization claim is valid.

For these use cases, itsyou.online supports JWT [RFC7519](https://tools.ietf.org/html/rfc7519).

## Refreshing a jwt

The standard oauth2 specification declares a refresh_token as a kind of special API key that is returned upon getting the oauth token or id token.
While this allows getting an entirely new token with the original scopes, it does have some drawbacks:

* The refresh token needs to be stored separately and if authorizations are passed to third party systems, the refresh token needs to be passed along as well if they are longer running services.
* No way of limiting the scopeset when passing a refresh token to someone else.

ItsYou.online puts the refresh token in the jwt itself, allowing to refresh the token without needing a separate refresh token. In order to include a refresh token in a jwt, one should ask for the `offline_access` scope. A `refresh_token` claim is inserted in the returned jwt.
To refresh it, just call `/v1/oauth/jwt/refresh` with the expired jwt as a bearer token in the Authorization header and you get a new one if the authorization still stands.

```
curl -H "Authorization: bearer OLD-JWT-TOKEN" https://itsyou.online/v1/oauth/jwt/refresh
```
If some of the authorizations for this token were removed, they are no longer returned in the scopes of the newly generated jwt.

If a refresh token has not been used for more than 30 days it will no longer be valid.

## Acquiring a jwt

Itsyou.online supports several ways of obtaining JWTs:
1. Use an OAuth2 token for JWT creation where the JWT's claim set is a subset of the OAuth token's scopes.
2. Directly get a JWT instead of a normal OAuth token when following the OAuth2 grant type flows.
3. Create a new jwt by using an existing jwt as authentication/authorization

### Case 1: Use an OAuth2 token for JWT creation where the JWT's claim set is a subset of the OAuth token's scopes

Suppose you have an OAuth token OAUTH-TOKEN with the following scopes:

- user:memberof:org1
- user:memberof:org2
- user:address:billing

and you want to call a third party service that only needs to know if the user is member of org1, there is no need to expose the billing address you are authorized to see.

You can create a JWT like this:
```
curl -H "Authorization: token OAUTH-TOKEN" https://itsyou.online/v1/oauth/jwt?scope=user:memberof:org1
```

The `scope` parameter can be a comma separated list of scopes. Instead of a query parameter, an http `POST` can also be submitted to this url with the scope parameter as a form value.

The response will be a JWT with:
* Header

    ```
    {
      "alg": "ES384",
      "typ": "JWT"
    }
    ```

* Data

    ```
    {
      "username": "bob",
      "scope": "user:memberof:org1",
      "iss": "itsyouonline",
      "aud": ["CLIENTID"],
      "exp": 1463554314
    }
    ```

    - iss: Issuer, in this case "itsyouonline"
    - exp: Expiration time in seconds since the epoch. This is set to the same time as the expiration time of the OAuth token used to acquire this JWT.
    - aud: An array with at least 1 entry: the `client_id` of the OAuth token used to acquire this JWT.

    If the OAuth token is not for a user but for an organization application that authenticated using the client credentials flow, the `username` field is replaced with a `globalid` field containing the globalid of the organization.

* Signature

    The JWT is signed by itsyou.online. The public key to verify if this JWT was really issued by itsyou.online is
    ```
    -----BEGIN PUBLIC KEY-----
    MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAES5X8XrfKdx9gYayFITc89wad4usrk0n2
    7MjiGYvqalizeSWTHEpnd7oea9IQ8T5oJjMVH5cc0H5tFSKilFFeh//wngxIyny6
    6+Vq5t5B0V0Ehy01+2ceEon2Y0XDkIKv
    -----END PUBLIC KEY-----
    ```

In case the requested scopes are not available for your OAuth token or the token has expired, an http 401 status code is returned.


### Creating JWT's for other audiences

Audiences are implemented according to [RFC7519 section 4.1.3.](https://tools.ietf.org/html/rfc7519#section-4.1.3)

In case you want to pass on a JWT and your authorization to a different audience,
you can specify said audiences to the call for creating a JWT.

```
curl -H "Authorization: token OAUTH-TOKEN" https://itsyou.online/v1/oauth/jwt??scope=user:memberof:org1&aud=external1,external2
```

In this case, this results in the following JWT data

    ```
    {
      "username": "bob",
      "scope": "user:memberof:org1",
      "iss": "itsyouonline",
      "aud": [
            "CLIENTID",
            "external1",
            "external2"
            ]
      "exp": 1463554314
    }
    ```

The audience field is a list of audiences. It is only set in case the audiences are
explicitly requested in the call for creating the JWT. The extra audiences are not
required to be valid globalid's of organizations in itsyou.online.


### Case 2: Directly get a JWT instead of a normal oauth2 token when following the oauth2 grant type flows

When using 1 of the authorization flows explained in the [Authorization grant types](oauth2.md) documentation, it is also possible to directly get a JWT returned instead of an OAuth2 token itself.
Add the `response_type=id_token` and a `scope` parameter with the desired scopes to the `/v1/oauth/access_token` call to do this.
For example:
```
https://itsyou.online/v1/oauth/access_token?grant_type=client_credentials&client_id=CLIENT_ID&client_secret=CLIENT_SECRET&response_type=id_token&scope=user:memberof:org1&aud=external1
```

In this case, the scope parameter needs to be given to prevent consumers to accidentally handing out `user:admin` or `organization:owner` scoped tokens to third party services

As shown in the example. it is also possible to specify additional audiences in the `/v1/oauth/access_token` call.

If the request has `application/json` in the accept header, the response is a json structure containing the jwt:
```
{
    "access_token":"ABCDEFGH........ABCDEFGH"
}
```
If no `application/json` is present in the accept header, the mime-type is `application/jwt` and the response is the jwt itself.

### Case 3: Create a new jwt by using an existing jwt as authentication/authorization

When you have a jwt and want to create a new one with less scopes, for a different audience or remove the refresh_token (or a combination of these), the same call can be performed as in case 1 but with the jwt in the Authorization header instead of the access token.
Be sure to use `bearer` instead of `token` in the authorization header.
```
curl -H "Authorization: bearer ABCDEFGH........ABCDEFGH" https://itsyou.online/v1/oauth/jwt?scope=user:memberof:org1
```
If the supplied jwt has a refresh_token, the newly generated jwt has a fresh expiration time, regardless of the expiration time of the supplied jwt. If not, the expiration time of the newly generated jwt is set to the expiration time of the supplied jwt.

If a jwt with for example less scopes is created and the `offline_access` scope is requested, ityou.online keeps a reference to the parent jwt's authorization and this in effect creates a tree of refreshable authorizations. If a specific authorization is removed from a parent, it is removed from all children as well.
Note that it is not possible to create a jwt with a refresh_token using an jwt that does not have a refresh_token.

Consumers should be careful not to pass jwt's with a refresh_token to third party service since they can keep using this authorization for as long as consumer's authorization is valid. When passing a jwt to an external service, it is best to ask for a new jwt first and pass that one.

#### JWT expiration date ####

Although the expiration time can not be set directly, a `validity` query parameter can be set when acquiring a jwt. The value of this parameter is interpreted as the duration you want the jwt to be valid and is expressed in seconds. This value can only be used to reduce the default duration of one day, i.e. you can use this parameter to ask for a jwt that is valid for 5 minutes, but a request for a jwt that is valid for a week will be ignored (a jwt will still be handed out if the remainder of the request is valid, but it will have the default 1 day expiration). Usage of this parameter is optional, if it is absent, the default expiration of one day will be used.

The same `validity` parameter can also be set when refreshing the jwt (if the `offline_access` scope was requested initially). The same restrictions apply here as when the jwt is handed out initially. If a jwt was acquired with a custom validity period, but no validity period is specified when refreshing it, the refreshed jwt will have the default 1 day validity

### Storing the actual values of scopes in JWT

In order to reduce some roundtrips to ItsYou.Online, some of the actual information given by
some scopes can also be stored in the JWT. This information is stored as a separate scope
in the form of `[SCOPE]:ACTUAL_INFO`. To enable this behavior, the `store_info=true`
query parameter must be provided when requesting a JWT. Currently the following info
can be stored:
  * `user:name`
  * `user:email[:label]`
  * `user:phone[:label]`
  * `user:validated:email[:label]`
  * `user:validated:phone[:label]`
