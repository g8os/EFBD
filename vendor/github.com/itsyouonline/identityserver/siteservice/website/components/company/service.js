(function() {
    'use strict';


    angular.module("itsyouonlineApp").service("CompanyService",CompanyService);


    CompanyService.$inject = ['$http','$q'];

    function CompanyService($http, $q) {
        var apiURL =  'api/companies';

        return {
            create: create
        };

        function create(name, taxnr){
            return $http.post(apiURL, {globalid: name, taxnr: taxnr}).then(
                function(response) {
                    return response.data;
                },
                function(reason){
                    return $q.reject(reason);
                }
            );
        }

    }


})();
