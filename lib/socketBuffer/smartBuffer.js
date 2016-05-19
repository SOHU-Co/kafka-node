/**
 * A smart buffer to handle small but also large packets
 * Creates a relatively large buffer, which is reusable
 * This way, we avoid expensive operations such as Buffer.concat
 * The class will try to reuse already read data and will only resize
 * when a large packet arrives.
 *
 * @example <caption>Initialise the buffer with initial size (bytes) and resize factor (decimal) </caption>
 * var smartBuffer = new SmartBuffer(10 * 1024 * 1024, 0.2); 10mb buffer that will resize at 20%
 *
 * @param {Number} [initialCapacity?] initial allocate size - optional
 * @param {Number} [resizeFactor?] the percentage to increase the buffer when needed - optional
 *
 * DEFAULTS: initialSize = 1MB
 *           resizeFactor = 20%
 *
 * @constructor
 */
"use strict";
var SmartBuffer = (function () {
    function SmartBuffer(initialCapacity, resizeFactor) {
        var self = this;
        self.initialCapacity = initialCapacity || SmartBuffer.DEFAULT_INITIAL_CAPACITY;
        self.resizeFactor = resizeFactor || SmartBuffer.DEFAULT_RESIZE_FACTOR;
        //deprecated, after v5 use Buffer.alloc(initialSize);
        self.buffer = new Buffer(initialCapacity);
        self.bufferCapacity = self.buffer.length;
        self.readIndex = 0;
        self.writeIndex = 0;
        self.size = 0;
        self.resetIfNeed = function () {
            if (self.readIndex == self.writeIndex) {
                self.readIndex = 0;
                self.writeIndex = 0;
                self.size = 0;
            }
        };
        self.resizeIfNeed = function (newBufferSize) {
            if ((newBufferSize + self.writeIndex) >= self.bufferCapacity) {
                //you need to resize or move
                //check move
                if (self.size + newBufferSize < self.bufferCapacity) {
                    //copy existing buffer
                    //set readIndex = 0
                    //set writeIndex = currentLen;
                    self.buffer.copy(self.buffer, 0, self.readIndex, self.writeIndex);
                    self.readIndex = 0;
                    self.writeIndex = self.size;
                }
                else {
                    //resize, make sure to accommodate the space
                    self.resize(newBufferSize);
                }
            }
        };
        self.resize = function (additionalNeededSize) {
            var tempLen = self.bufferCapacity + additionalNeededSize;
            var newSize = tempLen + (tempLen * self.resizeFactor);
            var newBuff = new Buffer(newSize);
            self.buffer.copy(newBuff, 0, self.readIndex, self.size);
            self.readIndex = 0;
            self.writeIndex = self.size;
            self.buffer = newBuff;
            self.bufferCapacity = newSize;
        };
    }
    SmartBuffer.prototype.write = function (buf) {
        var self = this;
        //used on read
        //self.resetIfNeed();
        if (buf == null) {
            console.log("SMARTBUFFER WARNING - write: buf == null");
            return;
        }
        if (buf.length > SmartBuffer.MAX_WRITE_BYTES) {
            console.log("SMARTBUFFER WARNING - write: max write length overpassed!", SmartBuffer.MAX_WRITE_BYTES);
            return;
        }
        //resize the buffer if needed - move data to beginning or create a new buffer
        self.resizeIfNeed(buf.length);
        //copy the new buffer to the large buffer
        buf.copy(self.buffer, self.writeIndex, 0, buf.length);
        self.writeIndex += buf.length;
        self.size = self.writeIndex - self.readIndex;
    };
    ;
    SmartBuffer.prototype.read = function (size, incrementPointer) {
        if (incrementPointer === void 0) { incrementPointer = true; }
        var self = this;
        if (self.size < size) {
            console.log("SMARTBUFFER ERROR - read: self.writeIndex - self.readIndex < size");
            return null;
        }
        var res = self.buffer.slice(self.readIndex, size);
        if (incrementPointer) {
            self.readIndex += size;
            self.resetIfNeed();
        }
        return res;
    };
    ;
    SmartBuffer.MAX_WRITE_BYTES = 1024 * 1024 * 500;
    SmartBuffer.DEFAULT_INITIAL_CAPACITY = 1024 * 1024; //1MB
    SmartBuffer.DEFAULT_RESIZE_FACTOR = 0.2; //20%
    return SmartBuffer;
}());
exports.SmartBuffer = SmartBuffer;