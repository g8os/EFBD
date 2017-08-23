# Reset Paswword Procedure

## Request Password Recovery
At login screen user was the option to press forget password link.
Users enters username or validated emailaddress.
UI does a POST `/login/forgotpassword` (unauthenticated)
```
{"login": "username or email"}
```

### API affects:
When a username is entered API sends a password reset link to all verified emails of this user.
If the user does not have a verified email address the API returns 409 and the user is locked out.

When email is entered API sends a password rest link only to this email if it is a verified email else a 409.

creates a reset token which is valid for only 10 minutes and sends an url in the form of `https://itsyou.online/login#/resetpassword?resettoken={token}`

## Email Verifiacation

Users click on the url in the email

UI asks for username or email + new password (with verification) and makes a POST request to `/login/resetpassword`

Example body:
```
{"user" : "email or username",
 "token": "token",
 "newpassword": "new valid password
}
```

API validates token and username combination and resets password.
User is able to login again.

