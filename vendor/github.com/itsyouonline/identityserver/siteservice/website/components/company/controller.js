(function() {
    'use strict';

    angular.module("itsyouonlineApp").controller("CompanyController",CompanyController);


    CompanyController.$inject = ['$location', 'CompanyService'];

    function CompanyController($location, CompanyService) {
        var vm = this;
        vm.create = create;

        vm.validationerrors = {};

        activate();

        function activate() {

        }

        function create(){
            vm.validationerrors = {};
            CompanyService.create(vm.name,vm.taxnr)
            .then(
                function(){
                    $location.path("/companies/" + vm.name);
                },
                function(reason){
                    if (reason.status == 409) {
                         vm.validationerrors = {duplicate: true};
                    }
                }
            );
        }
    }


})();
