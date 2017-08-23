(function () {
    'use strict';
    angular.module('itsyouonline.footer', [])
        .directive('itsYouOnlineFooter', ['footerService', function (footerService) {
            return {
                restrict: 'E',
                replace: true,
                templateUrl: 'components/shared/directives/footer.html',
                link: function (scope, element, attr) {
                  scope.showFooter = footerService.shouldShowFooter;
                }
            };
        }]);
})();
