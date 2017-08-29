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
                        case 412:
                            $scope.form.login.$setValidity("no_email_addresses", false);
                            break;
                        case 409:
                            $scope.form.login.$setValidity("has_validated_email", false);
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
            $scope.form.login.$setValidity("no_email_addresses", true);
            $scope.form.login.$setValidity("has_validated_email", true);
            $scope.form.login.$setValidity("invalid", true);
            $scope.form.login.$setValidity("generic", true);
        }
    }
})();
