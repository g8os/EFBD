(function () {
    'use strict';
    angular.module('loginApp')
        .controller('validateEmailController', ['$http', '$scope', validateEmailController]);

    function validateEmailController($http, $scope) {
        var vm = this;
        vm.submit = submit;
        vm.clearValidation = clearValidation;
        vm.emailSend = false;

        function submit() {
            var data = {
                username: vm.login,
                langkey: localStorage.getItem('langKey')
            };
            $http.post('/login/validateemail', data).then(
                function () {
                    vm.emailSend = true;
                },
                function (response) {
                    switch (response.status) {
                        case 404:
                            $scope.form.login.$setValidity("invalid", false);
                            break;
                        case 400:
                            $scope.form.login.$setValidity("generic", false);
                            break;
                    }
                }
            );
        }

        function clearValidation() {
            $scope.form.login.$setValidity("invalid", true);
            $scope.form.login.$setValidity("generic", true);
        }
    }
})();
