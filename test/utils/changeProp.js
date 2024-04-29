'use strict';

const TestServer = require('service-runner/test/TestServer');
const P          = require('bluebird');
const common     = require('./common');

const CHANGE_PROP_STOP_DELAY = 1000;
const CHANGE_PROP_START_DELAY = 30000;

class ChangePropServer extends TestServer {
    constructor(configPath = `${ __dirname }/../../config.test.yaml`) {
        super(configPath);
    }

    start() {
        return super.start()
        .delay(process.env.MOCK_SERVICES ? 0 : CHANGE_PROP_START_DELAY);
    }

    stop() {
        if (this._running) {
            return super.stop()
            .tap(() => common.clearKafkaFactory())
            .delay(process.env.MOCK_SERVICES ? 0 : CHANGE_PROP_STOP_DELAY);
        }
        return P.resolve();
    }
}

module.exports = ChangePropServer;
