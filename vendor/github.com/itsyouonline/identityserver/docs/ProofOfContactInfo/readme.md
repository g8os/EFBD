# Proof of contact information

So you created an identity. That's cool but only useful for a limited number of use cases. A randomly generated key and a username can be enough for some services to let you authenticate and link you to your documents on their site but we can do so much more.
For example, when you want to communicate with people, they will need some information on the identifiers on the channel through which they want to contact you. A phone number is a good example, you can have multiple depending on your context, a mobile phone, a fixed line at home and at work,...

First things first, let's link your work phone number to your identity.

![Link phone number to identity](LinkToPhoneNumber.svg)

This is easy but anyone can link the same phone number to it's identity, you want someone to validate it so you can prove this is your phone number.
Ask a phone number validation service (reallycoolphonenumbervalidationservice) to validate your phone number. Reallycoolphonenumbervalidationservice is a proper service that does it's best in what it does, validating phone numbers. You  send it a validation request and sign it with your identity private key. The phone number validation service validates the phone number, possibly by asking you some information related to your identity and signs the link between your identity and your phonenumber.
