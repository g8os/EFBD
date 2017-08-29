# Iyo see UI refactor

In the current implementation, iyo see documents can be written, updated and signed by an organization holding an access token or jwt 
with an `user:see` scope. Altough this makes sense, the UI exposes these documents under the organization itself. Therefore,
the user must necessarily be a member of the organization to be able to view these documents.

To improve usability (and to better represent their function), the IYO see documents must be exposed on a page next to the user's profile.
Thus no more organization membership is required to see the documents (e.g. if I sign a contract with an organization, I must not 
necessarily be a part of that organization...).

TODO:
  - create a link in the header (under the person icon) to IYO See (between `Shared information` and `Logout`). 
  The linked page will display all the user's organizations that have written see documents
  - after clicking on an organization, a page is shown with all the see documents written by the organization for that user.
  This page is similar to the one currently exposed under the organization for that user.
  - same functionality should apply here: all documents are shown, though only the latest version and a short description.
  For every document, the full history can be displayed, as well as the full description
