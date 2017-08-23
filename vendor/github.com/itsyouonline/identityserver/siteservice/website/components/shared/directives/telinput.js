(function() {
    'use strict';
    angular.module('itsyouonline.telinput', ['pascalprecht.translate'])
        .directive('telinput', ['countries', '$translate', '$http', '$q', function(countries, $translate, $http, $q) {
            return {

                restrict: 'E',
                replace: true,
                scope: {
                  number: '=phonenumber'
                },
                templateUrl: 'components/shared/directives/telinput.html',
                link: function (scope, element, attr) {
                    scope.countries = countries;
                    scope.country = {};
                    scope.sms = "";
                    scope.prefCountry = "";
                    scope.validationerrors = {};

                    scope.updateSMS = updateSMS;
                    scope.isNumeric = isNumeric;

                    init();

                    function init() {
                        // check if a valid international number is prefilled
                        if (scope.number) {
                            for (var i = 0; i < countries.length; i++) {
                                if (scope.number.startsWith(countries[i].dial_code)){
                                    scope.country = countries[i];
                                    scope.sms = scope.number.slice(countries[i].dial_code.length);
                                    return;
                                }
                            }
                        }
                        // if there is no valid number get the location
                        getLocation().then(
                            function(data) {
                                scope.prefCountry = data.location;
                                fillPrefCountry();
                            }
                        );
                    }

                    // try to fill in te preffered country
                    function fillPrefCountry() {
                        if (scope.prefCountry && !scope.number) {
                            for (var i = 0; i < countries.length; i++) {
                                if (countries[i].code == scope.prefCountry){
                                    scope.country = countries[i];
                                    break;
                                }
                            }
                        }
                    }

                    function updateSMS() {
                      scope.validationerrors.pattern = false;
                      scope.number = scope.country.dial_code + scope.sms;
                      if (!isNumeric(scope.number)) {
                          scope.validationerrors.pattern = true;
                      }
                    }

                    function isNumeric(n) {
                        return !isNaN(parseFloat(n)) && isFinite(n);
                    }

                    function getLocation() {
                        var url = '/location';
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
            }
        }])
})();
