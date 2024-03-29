'use strict';

const assert = require('assert');
const Rule = require('../../lib/rule');

describe('Rule', function () {

    it('topic required', function () {
        assert.throws(function () {
            const r = new Rule('rule'); // eslint-disable-line no-unused-vars
        }, Error, 'The rule should need to have a topic!');
    });

    it('no-op rule', function () {
        const r = new Rule('noop_rule', { topic: 'nono' });
        assert.ok(r.noop, 'The rule should be a no-op!');
    });

    it('simple rule - one request', function () {
        const r = new Rule('rule', {
            topic: 'nono',
            exec: { uri: 'a/b/c' }
        });
        assert.ok(Array.isArray(r.getHandler(0).exec), 'exec is expected to be an array!');
    });

    it('simple rule - multiple requests', function () {
        const r = new Rule('rule', {
            topic: 'nono',
            exec: [
                { uri: 'a/b/c' },
                { uri: 'e/f/g/h' }
            ]
        });
        assert.equal(r.getHandler(0).exec.length, 2, 'exec is expected to have 2 elements!');
    });

    describe('Matching', function () {

        const msg = {
            meta: {
                uri: 'a/fake/uri/for/you',
                request_id: '12345678-9101'
            },
            number: 1,
            string: 'oolala'
        };

        it('all', function () {
            const r = new Rule('rule', {
                topic: 'nono',
                exec: { uri: 'a/b/c' }
            });
            assert.equal(r.test(msg), 0, 'Expected the rule to match all event messages!');
        });

        it('simple value match', function () {
            const r = new Rule('rule', {
                topic: 'nono',
                exec: { uri: 'a/b/c' },
                match: { number: 1, string: 'oolala' }
            });
            assert.equal(r.test(msg), 0, 'Expected the rule to match the given message!');
        });

        it('simple value mismatch', function () {
            const r = new Rule('rule', {
                topic: 'nono',
                exec: { uri: 'a/b/c' },
                match: { number: 2, string: 'oolala' }
            });
            assert.equal(r.test(msg), -1, 'Expected the rule not to match the given message!');
        });

        it('regex match', function () {
            const r = new Rule('rule', {
                topic: 'nono',
                exec: { uri: 'a/b/c' },
                match: { number: 1, string: '/(?:la)+/' }
            });
            assert.equal(r.test(msg), 0, 'Expected the rule to match the given message!');
        });

        it('regex match with undefined', function () {
            const r = new Rule('rule', {
                topic: 'nono',
                exec: { uri: 'a/b/c' },
                match: { number: 1, string: '/.+/' }
            });
            const msgWithUndefined = Object.assign({}, msg);
            msgWithUndefined.string = undefined;
            assert.equal(r.test(msgWithUndefined), -1, 'Expected the rule not to match the given message!');
        });

        it('regex mismatch', function () {
            const r = new Rule('rule', {
                topic: 'nono',
                exec: { uri: 'a/b/c' },
                match: { number: 1, string: '/lah/' }
            });
            assert.equal(r.test(msg), -1, 'Expected the rule not to match the given message!');
        });

        it('array match', function () {
            const r = new Rule('rule', {
                topic: 'nono',
                exec: { uri: 'a/b/c' },
                match: { array: ['1', 2, '/(\\d)/'] }
            });
            const msgToMatch = { array: [2, '1', '3', '4', 5] };
            assert.equal(r.test(msgToMatch), 0, 'Expected the rule to match the given message!');
        });

        it('malformed match', function () {
            assert.throws(
                function () {
                    // eslint-disable-next-line no-unused-vars
                    const r = new Rule('rule', {
                        topic: 'nono',
                        exec: { uri: 'a/b/c' },
                        match: { number: 1, string: '/l/ah/' }
                    });
            }, Error);
        });

        it('match_not', function () {
            const r = new Rule('rule', {
                topic: 'nono',
                exec: { uri: 'a/b/c' },
                match_not: { meta: { uri: '/my-url/' } }
            });
            assert.equal(r.test(msg), 0, 'Expected the rule to match the given message!');
        });

        it('match_not array', () => {
            const r = new Rule('rule', {
                topic: 'nono',
                exec: { uri: 'a/b/c' },
                match_not: [
                    { meta: { uri: '/my-url/' } },
                    { meta: { uri: '/my-other-url/' } }
                ]
            });
            assert.equal(r.test(msg), 0, 'Expected the rule to match the given message!');
            assert.equal(r.test({
                meta: {
                    uri: '/my-url/'
                }
            }), -1, 'Expected the rule not to match the given message!');
            assert.equal(r.test({
                meta: {
                    uri: '/my-other-url/'
                }
            }), -1, 'Expected the rule not to match the given message!');
        });

        it('matches match and match_not', function () {
            const r = new Rule('rule', {
                topic: 'nono',
                exec: { uri: 'a/b/c' },
                match: { number: 1 },
                match_not: { meta: { uri: '/my-url/' } }
            });
            assert.equal(r.test(msg), 0, 'Expected the rule to match the given message!');
        });

        it('matches match but not match_not', function () {
            const r = new Rule('rule', {
                topic: 'nono',
                exec: { uri: 'a/b/c' },
                match: { number: 1 },
                match_not: { meta: { uri: '/fake/' } }
            });
            assert.equal(r.test(msg), -1, 'Expected the rule not to match the given message!');
        });

        it('matches match_not but not match', function () {
            const r = new Rule('rule', {
                topic: 'nono',
                exec: { uri: 'a/b/c' },
                match: { number: 10 },
                match_not: { meta: { uri: '/my-url/' } }
            });
            assert.equal(r.test(msg), -1, 'Expected the rule not to match the given message!');
        });

        it('matches match but is canary event and should_discard_canary_events is true', function () {
            const canaryMessage = {
                meta: {
                    uri: 'a/fake/uri/for/you',
                    request_id: '12345678-9101',
                    domain: 'canary'
                },
                number: 1,
                string: 'oolala'
            };

            const r = new Rule('rule', {
                topic: 'nono',
                exec: { uri: 'a/b/c' },
                match: { number: 1 },
                discard_canary_events: true
            });
            assert.equal(r.test(canaryMessage), -1, 'Expected the rule not to match the given canary event.');
        });

        it('matches match and is canary event and should_discard_canary_events is false', function () {
            const canaryMessage = {
                meta: {
                    uri: 'a/fake/uri/for/you',
                    request_id: '12345678-9101',
                    domain: 'canary'
                },
                number: 1,
                string: 'oolala'
            };

            const r = new Rule('rule', {
                topic: 'nono',
                exec: { uri: 'a/b/c' },
                match: { number: 1 },
                discard_canary_events: false
            });
            assert.equal(r.test(canaryMessage), 0, 'Expected the to match the given canary event.');
        });

        it('expansion', function () {
            const r = new Rule('rule', {
                topic: 'nono',
                exec: { uri: 'a/{match.meta.uri[1]}/c' },
                match: { meta: { uri: '/\\/fake\\/([^\\/]+)/' }, number: 1 }
            });
            const exp = r.getHandler(r.test(msg)).expand(msg);
            assert.deepEqual(exp.meta.uri, /\/fake\/([^/]+)/.exec(msg.meta.uri));
        });

        it('expansion with named groups', function () {
            const r = new Rule('rule', {
                topic: 'nono',
                exec: { uri: 'a/{match.meta.uri.element}/c' },
                match: { meta: { uri: '/\\/fake\\/(?<element>[^\\/]+)/' }, number: 1 }
            });
            const exp = r.getHandler(r.test(msg)).expand(msg);
            assert.deepEqual(exp.meta.uri, { element: 'uri' });
        });

        it('checks for named and unnamed groups mixing', function () {
            try {
                // eslint-disable-next-line no-unused-vars
                const r = new Rule('rule', {
                    topic: 'nono',
                    exec: { uri: 'a/{match.meta.uri.element}/c' },
                    // eslint-disable-next-line no-useless-escape
                    match: { meta: { uri: '/\\/(\w+)\\/(?<element>[^\\/]+)/' }, number: 1 }
                });
                throw new Error('Error must be thrown');
            } catch (e) {
                assert.deepEqual(e.message,
                    'Invalid match regex. Mixing named and unnamed capture groups are not supported. Regex: /\\/(w+)\\/(?<element>[^\\/]+)/');
            }
        });

    });

});
