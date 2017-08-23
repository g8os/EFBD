
## UI

### starting from department

- info under: itsyou.online/see/$orgname
- url e.g. itsyou.online/see/threefold.public
- last name is name of organization
- if not used then you see all organizations (v1.1)

### starting from all : v1.1

- info under: itsyou.online/see
- person will then see all his org's where there are items available
- click's on org will see the detail of these items for that org

### functions on UI

- see items in grid alike view
- grid can be filtered on: category, postdate
- show short markdown content in the grid
- ability to further expand the short markdown to the full content
- showed fields in grid
    - category
    - short descr
    - post date
    - expiration
- should also be possible to see history of 1 item (based on the versions & sorted on post date)
    - e.g. terms of conditions for hosters of TFF, can see list of all versions
    - show which specific version I have signed and which ones not signed yet


## API

- Make it possible that an organization adds a URL to a document to IYOSEE

- Make it accessible for the user
  - GET `https://itsyou.online/users/<userid>/see`: list all organizations that added something to this user's `see`
  - GET `https://itsyou.online/users/<userid>/see/<organization>` to list all documents added by this organization
  - POST `https://itsyou.online/users/<userid>/see/<organization>/<uniqueid>` to save a link
  - PUT `https://itsyou.online/users/<userid>/see/<organization>/<uniqueid>` to update a link
  
@TODO DELETE?

User can't add his own links

also need scope to allow organizations to write to this


## direct link to info

- url e.g. itsyou.online/see/threefold.public/uniqueid.pdf
- url e.g. itsyou.online/see/threefold.public/uniqueid.docx (the extension is only required for mime download)

## what is the info which can be registered to IYO see

- link
- category
- uniqueid (name)
- version 
- markdown short descr
- markdown full descr 
- validity_date: start_date / end_date
- post_date: data that info was uploaded
- required security level
- original_content_url
- signature (with priv key of user on rogerthat app) of all content above (e.g. json)

## example process

### mini app on rogerthat: accept arrival of a box

- when the user accepts the arrival of box we ask him to accept the terms & conditions (send above)
- scan QR code which links to original order
- terms & conditions link is shown to user (for iyosee)
- app asks if the user has read the link & accepts
- the app submits the signature to IYO for the content already shown above

remark: the private key for signing is stored in rogerthat


