(function () {
    'use strict';
    angular
        .module('loginApp')
        .controller('smsConfirmationController', ['$cookies', '$http', '$timeout', '$window', '$scope', smsConfirmationController]);

    function smsConfirmationController($cookies, $http, $timeout, $window, $scope) {
        var vm = this;
        vm.submit = submit;
        vm.smsconfirmation = {confirmed: false};

        $timeout(checkconfirmation, 1000);

        function checkconfirmation() {
            $http.get('login/smsconfirmed').then(
                function success(response) {
                    vm.smsconfirmation = response.data;
                    if (!response.data.confirmed) {
                        $timeout(checkconfirmation, 1000);
                    } else {
                        submit();
                    }
                },
                function failed() {
                    $timeout(checkconfirmation, 1000);
                }
            );
        }

        function submit() {
            var query = $window.location.search;
            var data = {
                smscode: vm.smscode
            };
            $http
                // append the query to this call so we don't drop out of an oauth flow
                .post('login/smsconfirmation' + query, data)
                .then(function (response) {
                    $window.location.href = response.data.redirecturl;
                    $cookies.remove('registrationdetails');
                }, function (response) {
                    switch (response.status) {
                        case 422:
                            $scope.phoneconfirmationform.smscode.$setValidity("invalid_sms_code", false);
                            break;
                    }
                });
        }

    }
})();
