(function () {
    'use strict';

    angular
        .module("itsyouonline.registration")
        .service("registrationService", ['$http', RegistrationService]);

    function RegistrationService($http) {
        return {
            validateUsername: validateUsername,
            register: register,
            getLogo: getLogo,
            getDescription: getDescription
        };

        function validateUsername(username) {
            var options = {
                params: {
                    username: username
                }
            };
            return $http.get('/validateusername', options);
        }

        function register(twoFAMethod, login, email, password, totpcode, sms, redirectparams) {
            var url = '/register?' + redirectparams;
            var data = {
                twofamethod: twoFAMethod,
                login: login.trim(),
                email: email.trim(),
                password: password,
                totpcode: totpcode,
                phonenumber: sms,
                redirectparams: redirectparams,
                langkey: localStorage.getItem('langKey')
            };
            return $http.post(url, data);
        }

        function getLogo(globalid) {
            var url = '/api/organizations/' + encodeURIComponent(globalid) + '/logo';
            return $http.get(url).then(
                function (response) {
                    return response.data;
                },
                function (reason) {
                    return $q.reject(reason);
                }
            );
        }


        function getDescription(globalId, langKey) {
            var url = '/api/organizations/' + encodeURIComponent(globalId) + '/description/' + encodeURIComponent(langKey) + '/withfallback';
            return $http.get(url).then(
                function (response) {
                    return response.data;
                },
                function (reason) {
                    return $q.reject(reason);
                }
            );
        }
    }
})();
