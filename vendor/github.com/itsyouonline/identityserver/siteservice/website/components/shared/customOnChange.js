(function customOnChange() {
    'use strict'
    angular.module('itsyouonline.shared')
        .directive('customOnChange', function() {
            return {
                restrict: 'A',
                link: function (scope, element, attrs) {
                    var onChangeHandler = scope.$eval(attrs.customOnChange);
                    element.bind('change', onChangeHandler);
                }
            };
        })
})();
