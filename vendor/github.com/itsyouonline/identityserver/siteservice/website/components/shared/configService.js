(function () {
    'use strict';
    angular.module('itsyouonline.shared')
        .service('configService', ['$http', configService]);

    function configService($http) {
        var config;
        return {
            getConfig: getConfig
        };

        function getConfig(callback) {
            if (!config) {
                $http.get('/config').then(function (response) {
                    config = response.data;
                    callback(config);
                });
            } else {
                callback(config);
            }
        }
    }
})();