(function () {
    'use strict'
    angular.module('itsyouonlineApp')
        .directive('tree', function() {
            return {
                restrict: 'E',
                replace: true,
                scope: {
                  t: '=src'
                },
                templateUrl: 'components/user/directives/treeDirective.html',
                link: function(scope, element, attrs) {

                    scope.collapseChildren = collapseChildren;
                    scope.hasChildren = hasChildren;

                    function collapseChildren(org) {
                        org.childrenCollapsed = !org.childrenCollapsed;
                    }

                    function hasChildren(org) {
                       var i = 0;
                       for (var key in org.children) {
                          i++;
                       }
                       return i > 0;
                    }

                }
            };
        })
})();
