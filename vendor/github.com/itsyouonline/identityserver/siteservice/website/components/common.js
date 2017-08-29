function themingConfig($mdThemingProvider) {
    $mdThemingProvider.definePalette('blueish', {
        '50': '#f7fbfd',
        '100': '#badeed',
        '200': '#8ec8e2',
        '300': '#55add3',
        '400': '#3ca1cd',
        '500': '#4d738a',
        '600': '#2a7ea3',
        '700': '#236b8a',
        '800': '#1d5872',
        '900': '#17455a',
        'A100': '#f7fbfd',
        'A200': '#badeed',
        'A400': '#3ca1cd',
        'A700': '#236b8a',
        'contrastDefaultColor': 'light',
        'contrastDarkColors': '50 100 200 300 400 A100 A200 A400'
    });
    $mdThemingProvider
        .theme('default')
        .primaryPalette('blueish');
}

function httpConfig($httpProvider) {
    $httpProvider.interceptors.push('authenticationInterceptor');
    //initialize get if not there
    if (!$httpProvider.defaults.headers.get) {
        $httpProvider.defaults.headers.get = {};
    }
    //disable IE ajax request caching
    $httpProvider.defaults.headers.get['If-Modified-Since'] = '0';
}

function authenticationInterceptor($q, $window) {
    return {
        'responseError': function (response) {
            if (response.status === 401 || response.status === 403 || response.status === 419) {
                if ($window.location.href.indexOf('/register') != -1) {
                    $window.location.href = '/register';
                } else {
                    $window.location.href = '/login';
                }
            } else if (response.status.toString().startsWith('5')) {
                $window.location.href = 'error' + response.status;
            }

            return $q.reject(response);
        }
    };
}

function translateConfig($translateProvider) {
    $translateProvider.useStaticFilesLoader({
        prefix: 'assets/i18n/',
        suffix: '.json'
    });
    // use 'escaped' since 'sanitize' doesn't aggree with the cyrillic charset
    $translateProvider.useSanitizeValueStrategy('escaped');
    $translateProvider.useMissingTranslationHandlerLog();

    var supportedLangs = ["en", "nl", "ru"];
    var defaultLang = "en";

    // selectedLangKey is the language key that has explicitly been selected by the user
    var langKey = localStorage.getItem('selectedLangKey');
    // set the langKey, this is the sites language, to the selected language. if its null, it'll be overriden anyway
    localStorage.setItem('langKey', langKey);
    // it the user hasn't set a language yet
    if (!langKey) {
        var langParam = getParameterByName("lang");
        var lang = langParam || URI(window.location.href).search(true).lang;
        // if a queryvalue 'lang' is set and within the supported languages use that
        if (supportedLangs.indexOf(lang) > -1) {
            localStorage.setItem('langKey', lang);
            // Store the langkey requested through the url params
            localStorage.setItem('requestedLangKey', lang);
            langKey = lang;
        } else {
            var previousLang = localStorage.getItem('requestedLangKey');
            // if a language was set thourgh an URL in a previous request use that
            if (previousLang) {
                localStorage.setItem('langKey', previousLang);
                langKey = previousLang;
            } else {
                // if no language was set and nothing is requested, try to check the browser language...
                browserLang = getBrowserLang();
                if (supportedLangs.indexOf(browserLang) >-1) {
                    localStorage.setItem('langKey', browserLang);
                    langKey = browserLang;
                }
                //if all else fails just use English
                localStorage.setItem('langKey', defaultLang);
                langKey = defaultLang;
            }
        }
    }
    $translateProvider.use(langKey);
}

function getBrowserLang() {
    browserLang = navigator.userLanguage || navigator.language;
    // only return the initial language code
    return browserLang.slice(0,2);
}

function getParameterByName(name, url) {
    if (!url) {
          url = window.location.href;
    }
    name = name.replace(/[\[\]]/g, "\\$&");
    var regex = new RegExp("[?&]" + name + "(=([^&#]*)|&|#|$)"),
        results = regex.exec(url);
    if (!results) return null;
    if (!results[2]) return '';
    return decodeURIComponent(results[2].replace(/\+/g, " "));
}
