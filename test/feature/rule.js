'use strict';


var assert = require('assert');
var Rule = require('../../lib/rule');


describe('Rule', function() {

    it('topic required', function() {
        assert.throws(function() {
            var r = new Rule('rule');
        }, Error, 'The rule should need to have a topic!');
    });

    it('no-op rule', function() {
        var r = new Rule('noop_rule', {topic: 'nono'});
        assert.ok(r.noop, 'The rule should be a no-op!');
    });

    it('simple rule - one request', function() {
        var r = new Rule('rule', {
            topic: 'nono',
            exec: {uri: 'a/b/c'}
        });
        assert.ok(Array.isArray(r.exec), 'exec is expected to be an array!');
    });

    it('simple rule - multiple requests', function() {
        var r = new Rule('rule', {
            topic: 'nono',
            exec: [
                {uri: 'a/b/c'},
                {uri: 'e/f/g/h'}
            ]
        });
        assert.ok(r.exec.length === 2, 'exec is expected to have 2 elements!');
    });

    describe('Matching', function() {

        var msg = {
            meta: {
                uri: 'a/fake/uri/for/you',
                request_id: '12345678-9101'
            },
            number: 1,
            string: 'oolala'
        };

        it('all', function() {
            var r = new Rule('rule', {
                topic: 'nono',
                exec: {uri: 'a/b/c'}
            });
            assert.ok(r.test(msg), 'Expected the rule to match all event messages!');
        });

        it('simple value match', function() {
            var r = new Rule('rule', {
                topic: 'nono',
                exec: {uri: 'a/b/c'},
                match: {number: 1, string: 'oolala'}
            });
            assert.ok(r.test(msg), 'Expected the rule to match the given message!');
        });

        it('simple value mismatch', function() {
            var r = new Rule('rule', {
                topic: 'nono',
                exec: {uri: 'a/b/c'},
                match: {number: 2, string: 'oolala'}
            });
            assert.ok(!r.test(msg), 'Expected the rule not to match the given message!');
        });

        it('regex match', function() {
            var r = new Rule('rule', {
                topic: 'nono',
                exec: {uri: 'a/b/c'},
                match: {number: 1, string: '/(?:la)+/'}
            });
            assert.ok(r.test(msg), 'Expected the rule to match the given message!');
        });

        it('regex mismatch', function() {
            var r = new Rule('rule', {
                topic: 'nono',
                exec: {uri: 'a/b/c'},
                match: {number: 1, string: '/lah/'}
            });
            assert.ok(!r.test(msg), 'Expected the rule not to match the given message!');
        });

        it('malformed match', function() {
            assert.throws(
                function() {
                    var r = new Rule('rule', {
                        topic: 'nono',
                        exec: {uri: 'a/b/c'},
                        match: {number: 1, string: '/l\/ah/'}
                    });
            }, Error);
        });

        it('expansion', function() {
            var r = new Rule('rule', {
                topic: 'nono',
                exec: {uri: 'a/{match.meta.uri[1]}/c'},
                match: { meta: { uri: "/\\/fake\\/([^\\/]+)/" }, number: 1 }
            });
            var exp = r.expand(msg);
            assert.deepEqual(exp.meta.uri, /\/fake\/([^\/]+)/.exec(msg.meta.uri));
        });

    });

});

