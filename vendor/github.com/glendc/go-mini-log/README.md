# go-mini-log

This package is a modified version of the standard [pkg/log][pkglog] code.
The goal of this package is to provide a minimalistic package, which allows for the seperation
of _debug_ and _info_ statements. _Debug_ statements which are meant for the developer only,
while _info_ statements remain in production and thus can be seen by the user.

The standard logging package is works as it is, except for 2 points:

+ It does not provide the option to have verbose logging for debugging purposes only;
+ It has way to many logging levels, which are really not needed;

This package aims to resolve these issues by using what seems to work well,
adding to it what was missing, and removing what wasn't needed.

_Warnings_ are either _info_ or _error_,
so logging them as a seperate level doesn't make sense.

_Errors_ should be either handled or they are fatal.
If they are handled, it doesn't make sense to log them as errors any longer,
and should be logged as _info_ instead.

This package is not fancy,
and should be seen as a practical version of [pkg/log][pkglog].

[pkglog]:https://github.com/golang/go/tree/master/src/log
