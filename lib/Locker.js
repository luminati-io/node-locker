(function(module) {
    var net        = require("net"),
        util       = require("util"),
        events     = require("events"),
        Lock       = require("./Lock"),
        LockAction = require("./LockAction");

    function Locker(port, host) {
        var self = this;

        self.port     = port;
        self.host     = host;
        self.registry = {};
        self.sequence = 0;
        self.reconnect = false;
        self.connected = false;
        self.on_connect = [];

        self.connect = function(callback) {
            if (callback)
            {
                if (self.connected)
                    callback();
                else
                    self.on_connect.push(callback);
            }
            if (self.connection)
                return;

            var data = new Buffer(0),
                temp = new Buffer(0);

            self.connection = net.createConnection(port, host);

            self.connection.on("error", self.reset.bind(self, 1));
            self.connection.on("close", self.reset.bind(self, 2));
            self.connection.on("timeout", self.reset.bind(self, 3));

            self.connection.on("data", function(part) {
                var sequence,
                    action,
                    result,
                    lock;

                temp = new Buffer(data.length + part.length);
                data.copy(temp, 0);
                part.copy(temp, data.length);

                data = new Buffer(temp);

                while (data.length >= 6) {
                    sequence = data.readUInt32LE(0);
                    action   = data[4];
                    result   = data[5];

                    lock = self.registry[sequence];

                    if (lock) {
                        if (action == LockAction.ACTION_LOCK) {
                            if (lock.acquire) {
                                lock.acquire(result ? undefined : new Error("Failed to acquire lock"));
                            }
                        } else if (action == LockAction.ACTION_UNLOCK) {
                            if (lock.release) {
                                lock.release(result ? undefined : new Error("Failed to release lock"));
                            }
                        }
                    }

                    data = data.slice(6);
                }
            });

            self.connection.on("connect", ()=>{
                let cb;
                self.connected = true;
                self.connection.write(self.createRequest("", process.pid, 0, 0, self.reconnect ? LockAction.ACTION_CONT : LockAction.ACTION_INIT));
                self.reconnect = true;
                while (cb = self.on_connect.shift())
                    cb();
            });
        };

        self.reset = function(error) {
            if (error==2)
            {
                self.connection = self.connected = undefined;
                setTimeout(()=>self.connect(), 1000);
            }
        };

        self.createRequest = function(name, sequence, wait, timeout, type) {
            var name    = new Buffer(name),
                request = new Buffer(1 + 4 + 4 + 4 + 1 + name.length);

            request[0] = name.length;
            request.writeUInt32LE(sequence, 1);
            request.writeUInt32LE(wait, 5);
            request.writeUInt32LE(timeout, 9);
            request[13] = type;

            name.copy(request, 14);

            return request;
        };

        self.requestAction = function(name, sequence, wait, timeout, action) {
            self.connect(function() {
                self.connection.write(self.createRequest(name, sequence, wait, timeout, action));
            });
        };

        self.acquire = function(name, sequence, wait, timeout, callback) {
            self.requestAction(name, sequence, wait, timeout, LockAction.ACTION_LOCK);
            self.registry[sequence].acquire = callback;
        };

        self.release = function(sequence, callback) {
            self.requestAction("", sequence, 0, 0, LockAction.ACTION_UNLOCK);
            self.registry[sequence].release = callback;
        };

        self.createLock = function(name) {
            var sequence = ++this.sequence,
                acquire  = this.acquire,
                release  = this.release,
                registry = this.registry,
                lock     = new Lock(name, sequence, acquire, release);

            lock.on("error", function(error) {
                self.emit("error", error, lock);
            });

            registry[sequence] = {
                lock: lock
            };

            return lock;
        };
    };

    util.inherits(Locker, events.EventEmitter);

    Locker.prototype.locked = function(name, wait, timeout, callback) {
        var self = this,
            lock = self.createLock(name);

        lock.acquire(wait, timeout, function(error) {
            callback(error, lock.release.bind(lock));
        });
    };

    module.exports = Locker;
})(module);
