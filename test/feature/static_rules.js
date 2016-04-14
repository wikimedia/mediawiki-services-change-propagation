"use strict";

var ChangeProp = require('../utils/changeProp');

describe('Basic rule management', function() {
    var changeProp = new ChangeProp('config.test.yaml');

    before(function() {
        return changeProp.start();
    });

    after(function() { return changeProp.stop(); });
});