"use strict";

const HyperSwitch = require('hyperswitch');
const Template = HyperSwitch.Template;

class OresProcessor {
    constructor(options) {
        this._options = options;
        if (!options.templates.ores_precache_template) {
            throw new Error('OreProcessor is miconfigured. ores_precache_template is required');
        }
        this._ores_precache_template = new Template(options.templates.ores_precache_template);
    }

    process(hyper, req) {
        const context = {
            request: req,
            message: req.body
        };
        return hyper.request(this._ores_precache_template.expand(context))
        .then((res) => {
            const newMessage = {
                meta: {
                    topic: 'mediawiki.revision-score',
                    uri: context.message.meta.uri,
                    request_id: context.message.meta.request_id,
                    domain: context.message.meta.domain,
                },
                database: context.message.database,
                page_id: context.message.page_id,
                page_title: context.message.page_title,
                page_namespace: context.message.page_namespace,
                rev_id: context.message.rev_id,
                rev_parent_id: context.message.rev_parent_id,
                rev_timestamp: context.message.rev_timestamp,
                scores: []
            };
            const domainScores = res.body[newMessage.database];
            const revScores = domainScores.scores[`${newMessage.rev_id}`];
            Object.keys(revScores).forEach((modelName) => {
                newMessage.scores.push({
                    model_name: modelName,
                    model_version: domainScores.models[modelName].version,
                    prediction: revScores[modelName].prediction,
                    probability: revScores[modelName].probability
                });
            });
            return hyper.post({
                uri: '/sys/queue/events',
                body: [ newMessage ]
            });
        });
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
            process: processor.process.bind(processor),
        }
    };
};
