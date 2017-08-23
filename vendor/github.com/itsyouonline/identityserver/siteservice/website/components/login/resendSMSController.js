(function () {
    'use strict';
    angular
        .module('loginApp')
        .controller('resendSmsController', ['$scope', '$window', '$http', 'LoginService', resendSmsController]);

    function resendSmsController($scope, $window, $http, loginService) {
        var vm = this;
        vm.sms = "";
        vm.submit = submit;
        vm.resetValidation = resetValidation;

        function submit() {
            var data = {
                phonenumber: vm.sms.replace(' ', ''),
                langkey: localStorage.getItem('langKey')
            };
            $http
                .post('/login/resendsms', data)
                .then(function () {
                    $window.location.href = '#/smsconfirmation';
                }, function (response) {
                    switch (response.status) {
                        case 422:
                            if (response.data.error === 'invalid_phonenumber') {
                                $scope.phoneconfirmationform.phonenumber.$setValidity("invalid_phonenumber", false);
                            }
                            break;
                    }
                });
        }

        function resetValidation() {
            $scope.phoneconfirmationform.phonenumber.$setValidity("invalid_phonenumber", true);
        }
    }
})();
