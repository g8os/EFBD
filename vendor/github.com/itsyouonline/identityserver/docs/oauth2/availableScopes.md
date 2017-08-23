# What data can be managed when having an `admin` scope for a specific user

## /users/{username}

### `{username}` is the a user on which the `user:admin` scope is granted:

* `user:admin`

## /organizations/{globalid}

### User is owner of the organization

* `organization:owner`

### User is member of the organization

* `organization:member`

# Scopes that can be requested by an oauth client

## `user:name`

First name and last name of the user


## `user:memberof:<globalid>`

A client can check if a user is member or owner of an organization with a specific globalid.
Itsyou.online checks if the user is indeed member or owner and the user needs to confirm
that the requesting client is allowed to know that he/she is part of the organization.

If the user is no member of the <globalid> organization, the oauth flow continues but the scope will not be available. This scope can be requested multiple times.

## `user:address[:<label>]`


## `user:email[:<label>]`


## `user:phone[:<label>][:write]`

The `:write` extension gives an application full access(read, update, delete) to a phone number

## `user:github`

## `user:facebook`

## `user:bankaccount[:<label>]`

## `user:digitalwalletaddress:[<label>]:[<currency>]`

## `user:publickey[:<label>]`

## `user:owneroff:email:<emailaddress>`

Users need to share this verified email address to complete the authorization flow.
If a user registers during an oauth flow where this scope is requested, the email address is automatically filled in the registration screen.
