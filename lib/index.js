var q = require("q");
var Buffers = require("buffers");

var workQueues = require("./work-queues");
var interception = require("./interception");
var caching = require("./caching");

module.exports = middleware;

function middleware(options) {
    var maxAge = options.maxAge;
    
    var cache = caching.createCache(maxAge, options.backend);
    var queues = workQueues.createQueueSet();
    
    function handle(request, response, next) {
        if (request.method === "GET") {
            var requestId = request._cacheId = request.url;
            var queue = queues.get(requestId);
            queue.add(function() {
                return handleCacheableRequest(request, response, next);
            });
        } else {
            next();
        }
    };
    
    function handleCacheableRequest(request, response, next) {
        var requestId = request._cacheId;
        
        function handleNoEntry() {
            var value = cacheResponse(requestId, response);
            process.nextTick(next);
            return value;
        }
        
        function handleEntry(cachedValue) {
            response.writeHead(
                cachedValue.statusCode,
                cachedValue.headers
            );
            response.end(new Buffer(cachedValue.body, "base64"));
            return null;
        }
        
        return cache.getValue(requestId).then(function(cachedValue) {
            if (!cachedValue) {
                return handleNoEntry();
            } else {
                return handleEntry(cachedValue);
            };
        }, handleNoEntry);
    }
    
    return handle;
    
    function cacheResponse(requestId, response) {
        var deferred = q.defer();
        
        var cachedResponse = {
            headers: {}
        };
        var bodyChunks = new Buffers;
        
        interception.interceptMethods(response, {
            
            writeHead: function(statusCode, headers) {
                for (var name in headers) {
                    cachedResponse.headers[name] = headers[name];
                }
            },
            
            setHeader: function(name, value) {
                cachedResponse.headers[name] = value;
            },
            
            write: function(chunk, encoding) {
                if (!Buffer.isBuffer(chunk)) {
                    chunk = new Buffer(chunk, encoding);
                }
                bodyChunks.push(chunk);
            },
            
            end: function(chunk, encoding) {
                if (chunk) {
                    response.write(chunk, encoding);
                }
                cachedResponse.statusCode = response.statusCode;
                cachedResponse.body = bodyChunks.toString("base64");
                if (response.statusCode < 500) {
                    cache.setValue(requestId, cachedResponse).fin(function() {
                        deferred.fulfill(null);
                    });
                } else {
                    deferred.fulfill(null);
                }
                interception.callOriginal(response.end);
                return false;
                // TODO: at the moment, we don't cache errors at all.
                // We should be more explicit about how we handle errors
                // e.g. retry on every request, cache error (with different maxAge?)
            }
            
        });
        return deferred.promise;
    }
}

function getNow() {
    return new Date().getTime();
}
