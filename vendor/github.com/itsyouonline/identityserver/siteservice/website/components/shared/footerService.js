(function () {
    'use strict';
    angular.module('itsyouonline.shared')
        .service('footerService', ['$http', footerService]);

        function footerService(){
          var service = this;
          service.showFooter = true;
          function shouldShowFooter(){
            return service.showFooter;
          }
          function setFooter(value){
            service.showFooter = value;
          }
          return {
            shouldShowFooter:shouldShowFooter,
            setFooter:setFooter
          };
        }
})();
