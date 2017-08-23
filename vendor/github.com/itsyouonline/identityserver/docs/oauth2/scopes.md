## Scopes

Scopes work a bit differently then in most known OAuth2 identity providers since we want to have more control about which information is exposed to client applications.

Lets explain this with an example: Bob wants to buy a dogcollar at petshop.com. When checking out his shopping cart, Bob authenticates using itsyou.online. Petshop.com needs a billing and a shipping address and asks for the scope `user:address:billing,user:address:shipping` during the authorization flow as described in the [OAuth2](oauth2.md) documentation.

Bob has 3 addresses registered in itsyou.online:

```yaml
bob:
  address:
    home:
      ...
    work:
      ...
    motherinlaw:
      ...
```

During the authorization request, itsyou.online informs Bob that petshop.com needs two addresses, 1 for billing and 1 for shipping.

He chooses to disclose the address of his mother in law as shipping address since he knows she will be home during the delivery and uses his home address as billing address.

Now, Bob does not want to disclose the labels on the addresses off course since petshop.com has no business with the fact that the collar will be delivered at his mother in law. This scope mapping is saved as such and when petshop.com requests the user information on `/users/bob/info` using the oauth token acquired, the following information will be returned:

```json
{
    "username":"bob",
    "address":
        [
            {"billing":{...}},
            {"shipping":{...}}
        ]
}
```

with the *billing* and *shipping* address being his *home* and *motherinlaw* address respectively.


The same concept is applied on all labelled user properties.
