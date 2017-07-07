# Contributing to 0-Disk

## Submitting Issues

Not every contribution comes in the form of code. Submitting, confirming, and triaging issues is an important task for any project. At zero-os we use GitHub to track all project issues.

Documentation is important. In case you see missing documentation, inline or external, please make an issue to track the work, and optionally make a PR where you add the missing documentation yourself.

If you see a travis CI test fail at any point, report it, if not already reported. Attach a scoped raw log as well.

When you submit an issue related to a service or command line tool, please include the version number of the service/tool as well as OS information. Please be as detailed as possible and provide logs when possible.

Before submitting issues, please search open and closed issues, to ensure your issue is not already reported (and perhaps even resolved, in case you use an old release).

## Contribution Process

We have a 4 step process for contributions:

1. Commit changes to a new git branch.
2. When finished, squash your commits into as little commits as makes sense (`git rebase -i`).
3. Create a GitHub Pull Request for your change.
4. Perform a Code Review together with the 0-Disk maintainers on the pull request.

### Pull Request Requirements

1. **Tests:** To ensure high quality code and protect against future regressions, we require all code to be tested in some way.
2. **Green CI Tests:** We use [Travis CI](https://travis-ci.org/) to test all pull requests. We require these test runs to succeed on every pull request before being merged. You can run these tests locally using `make test`.
