function initializePolyfills() {
  if (!String.prototype.startsWith) {
      String.prototype.startsWith = function (searchString, position) {
          position = position || 0;
          return this.substr(position, searchString.length) === searchString;
      };
  }
  if (!String.prototype.includes) {
      String.prototype.includes = function (search, start) {
          'use strict';
          if (typeof start !== 'number') {
              start = 0;
          }

          if (start + search.length > this.length) {
              return false;
          } else {
              return this.indexOf(search, start) !== -1;
          }
      };
  }
  if (!Array.prototype.includes) {
        Object.defineProperty(Array.prototype, 'includes', {
            value: function(search, start) {

              // First check if the array is defined
              if (this == null) {
                throw new TypeError('"this" is null or not defined');
              }

              // Get the object of the array
              var arr = Object(this);
              // NOP zero fill right bitshift, actually just a cast to
              // a 32bit uint
              var length = arr.length >>> 0;
              // If array length is 0, it contains nothing so include returns
              // false per definition
              if (length === 0) {
                return false;
              }
              // Start searchin at the given index. Start at 0 (the beginning)
              // if no start index is defined
              var n = start | 0;

              // searchPtr is the actual search index. If the defined start (n) is bigger than
              // 0, use that (searchPtr = n). Else add the negative index to the length of the array
              // if searchPtr is negative after this, set searchPtr to 0
              var searchPtr = Math.max(n >= 0 ? n : length - Math.abs(n), 0);

              // Repeat from the calculated start index untill the end
              while (searchPtr < length) {
                if (arr[searchPtr] === search) {
                  return true;
                }
                searchPtr++;
              }

              // If we didn't find the result above, return false
              return false;
            }
      });
}
  if (!Array.prototype.find) {
      Object.defineProperty(Array.prototype, 'find', {
          value: function (predicate) {
              'use strict';
              if (this == null) {
                  throw new TypeError('Array.prototype.find called on null or undefined');
              }
              if (typeof predicate !== 'function') {
                  throw new TypeError('predicate must be a function');
              }
              var list = Object(this);
              var length = list.length >>> 0;
              var thisArg = arguments[1];
              var value;

              for (var i = 0; i < length; i++) {
                  value = list[i];
                  if (predicate.call(thisArg, value, i, list)) {
                      return value;
                  }
              }
              return undefined;
          }
      });
  }
}
