var Cache = function (expire) {
    this._expire = expire || 5000; // ms
    this._granularity = 1000; // ms
    this._store = {};
    this._recentAddedKeys = [];
    this._expireQueue = [];

    var that = this;
    setInterval(function () {
        that._expireQueue.push(that._recentAddedKeys);
        that._recentAddedKeys = [];
    }, this._granularity);

    setTimeout(function () {
        that.clean();
    }, this._expire)
};

Cache.prototype.set = function (key, value) {
    this._store[key] = value;
    this._recentAddedKeys.push(key);
};
Cache.prototype.get = function (key) {
    if (this._store[key])
        return this._store[key];
};
Cache.prototype.clean = function () {
    console.log(this._expireQueue);
    var that = this;
    var keys = this._expireQueue.shift();
    keys && keys.forEach(function (key) {
        that._store[key] && delete that._store[key];
    });

    // recurrence
    setTimeout(function () {
        that.clean();
    }, this._granularity);
};
Cache.prototype.flush = function () {
    this._store = {}
};


module.exports = Cache;
