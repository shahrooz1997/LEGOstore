import time

class ABD_Server:

    # file to log server times
    latency_breakdown = open("ABD_latency_breakdown_server.log", "w")


    @staticmethod
    def get(key, timestamp, cache, persistent, lock):
        # timestamp is of no use in case of ABD but sending to maintain the same input for all
        # TODO:
        # Replace it with per key reader writer lock.
        # Current customized lock is just for generic lock not key specific.
        # Ideally it should take key as input and do locking key wise

        _b_time = time.time()
        lock.acquire_read()
        value = cache.get(key)

        if value:
            lock.release_read()
            ABD_Server.latency_breakdown.write("get-cache:{}\n".format(time.time() - _b_time))
            return "OK" + "+:--:+" + str(value[0]) + "+:--:+" + str(value[1])

        _b_time = time.time()
        value = persistent.get(key)
        ABD_Server.latency_breakdown.write("get-persistant:{}\n".format(time.time() - _b_time))
        if value[0]:
            lock.release_read()
            return "OK" + "+:--:+" + str(value[0]) + "+:--:+" + str(value[1])

        lock.release_read()
        return "Failed" + "+:--:+" + "NONE" + "+:--:+" + "NO KEY FOUND"


    @staticmethod
    def put(key, value, timestamp, cache, persistent, lock):
        lock.acquire_write()
        _b_time = time.time()
        data = cache.get(key)
        revoked_values = []
        if data:
            current_value, current_timestamp = data
            if timestamp > current_timestamp:
                revoked_values.extend(cache.put(key, (value, timestamp)))
        else:
            revoked_values.extend(cache.put(key, (value, timestamp)))
        ABD_Server.latency_breakdown.write("put-cache:{}\n".format(time.time() - _b_time))
        
        _b_time = time.time()
        for key, value in revoked_values:
            persistent.put(key, value)
        ABD_Server.latency_breakdown.write("put-persistant:{}\n".format(time.time() - _b_time))

        lock.release_write()

        return {"status": "OK"}


    @staticmethod
    def get_timestamp(key, cache, persistent, lock):
        lock.acquire_read()
        _b_time = time.time()
        data = cache.get(key)
        ABD_Server.latency_breakdown.write("get-timestamp-cache:{}\n".format(time.time() - _b_time))
        if not data:
            _b_time = time.time()
            data = persistent.get(key)
            ABD_Server.latency_breakdown.write("get-timestamp-persistant:{}\n".format(time.time() - _b_time))

        if data[0]:
            current_value, current_timestamp = data
            lock.release_read()

            return {"status": "OK", "timestamp": current_timestamp}

        lock.release_read()

        return {"status": "Failed", "timestamp": "0-0"}
