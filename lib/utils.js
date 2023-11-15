'use strict';

const uuidv1 = require('uuid').v1;

const utils = {};

/**
 * Computes the x-triggered-by header
 *
 * @param {Object} event the event
 * @return {string}
 */
utils.triggeredBy = (event) => {
    let prevTrigger = event.triggered_by || '';
    if (prevTrigger) {
        prevTrigger += ',';
    } else {
        prevTrigger = `req:${event.meta.request_id},`;
    }
    return `${prevTrigger + event.meta.stream}:${event.meta.uri}`;
};

utils.requestId = () => uuidv1();

/**
 * Safely stringifies the event to JSON string.
 *
 * @param {Object} event the event to stringify
 * @return {string|undefined} stringified event or undefined if failed.
 */
utils.stringify = (event) => {
    try {
        return JSON.stringify(event);
    } catch (e) {
        return undefined;
    }
};

/**
 * From a list of regexes and strings, constructs a regex that
 * matches any item in the list
 *
 * @param {Array} list the list of regexes and strings to unify
 * @return {RegExp|undefined} the compiled regex or undefined
 */
utils.constructRegex = (list) => {
    if (!list || !Array.isArray(list) || list.length === 0) {
        return undefined;
    }
    const regex = list.map((regexString) => {
        regexString = regexString.trim();
        if (/^\/.+\/$/.test(regexString)) {
            return `(?:${regexString.slice(1, regexString.length - 1)})`;
        }
        // Compare strings, instead
        const slash = /^\//.test(regexString) ? '' : '/';
        return `(?:${slash}${decodeURIComponent(regexString)
        .replace(/[-[\]/{}()*+?.\\^$|]/g, '\\$&')}$)`;
    }).join('|');
    /* eslint-disable-next-line security/detect-non-literal-regexp */
    return new RegExp(regex);
};

module.exports = utils;
