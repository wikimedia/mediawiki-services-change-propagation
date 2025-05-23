'use strict';

const uuidv1 = require('uuid').v1;

class OresProcessor {
    constructor(options) {
        this._options = options;
        if (!options.ores_precache_uris || !Array.isArray(options.ores_precache_uris)) {
            throw new Error('OresProcessor is misconfigured. ores_precache_uris is required');
        }
        if (!options.event_service_uri) {
            throw new Error('OresProcessor is misconfigured. event_service_uri is required');
        }
    }

    async process(hyper, req) {
        const message = req.body;
        const preCache = await Promise.all(this._options.ores_precache_uris.map(uri => hyper.post({
            uri,
            headers: {
                'content-type': 'application/json'
            },
            body: message
        })));

        const responses = preCache.map((res) => {
            if (!req.query.postevent || !res.body) {
                return res;
            }
            const now = new Date();
            const newMessage = {
                $schema: '/mediawiki/revision/score/2.0.0',
                meta: {
                    stream: 'mediawiki.revision-score',
                    uri: message.meta.uri,
                    request_id: message.meta.request_id,
                    id: uuidv1({ msecs: now.getTime() }),
                    dt: now.toISOString(),
                    domain: message.meta.domain
                },
                database: message.database,
                page_id: message.page_id,
                page_title: message.page_title,
                page_namespace: message.page_namespace,
                page_is_redirect: message.page_is_redirect,
                performer: message.performer,
                rev_id: message.rev_id,
                rev_parent_id: message.rev_parent_id,
                rev_timestamp: message.rev_timestamp
            };
            // newMessage.scores and/or newMessage.errors
            // will be provided only if there are any
            // elements for those objects.
            // https://phabricator.wikimedia.org/T210465#4797591

            const domainScores = res.body[newMessage.database];
            const revScores = domainScores.scores[`${ newMessage.rev_id }`];
            Object.keys(revScores).forEach((modelName) => {
                // The example schema for the score object:
                //   {
                //     "awesomeness": {
                //       "model_name": "awesomeness"
                //       "model_version": "1.0",
                //       "prediction": ["yes", "mostly"],
                //       "probability": {
                //         "yes": 0.99,
                //         "mostly": 0.90,
                //         "hardly": 0.01
                //       }
                //     }
                //   }
                // {

                const score = {
                    [modelName]: {
                        model_name: modelName,
                        model_version: domainScores.models[modelName].version
                    }
                };
                if (revScores[modelName].error) {
                    Object.assign(score[modelName], revScores[modelName].error);
                    newMessage.errors = newMessage.errors || {};
                    newMessage.errors = Object.assign(newMessage.errors, score);
                } else {
                    const originalScore = revScores[modelName].score;
                    // Convert prediction to array of strings
                    let prediction = originalScore.prediction;
                    if (!Array.isArray(prediction)) {
                        prediction = [ prediction ];
                    }
                    score[modelName].prediction = prediction.map(p => `${ p }`);
                    // Convert probabilities to an array of name-value pairs.
                    score[modelName].probability = Object.keys(originalScore.probability)
                        .reduce((obj, probName) => Object.assign(
                            obj,
                            { [probName]: originalScore.probability[probName] }
                            ),
                        {});
                    newMessage.scores = newMessage.scores || {};
                    newMessage.scores = Object.assign(newMessage.scores, score);
                }
            });
            return hyper.post({
                uri: this._options.event_service_uri,
                headers: {
                    'content-type': 'application/json'
                },
                body: [ newMessage ]
            });
        });

        return Promise.all(responses);
    }
}

module.exports = (options) => {
    const processor = new OresProcessor(options);
    return {
        spec: {
            paths: {
                '/': {
                    post: {
                        summary: 'submit revision-create for processing',
                        operationId: 'process'
                    }
                }
            }
        },
        operations: {
            process: processor.process.bind(processor)
        }
    };
};
