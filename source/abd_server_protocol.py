class ABDServer:

    @staticmethod
    def get(key, timestamp, cache, persistent, lock):
        # timestamp is of no use in case of ABD but sending to maintain the same input for all
        # TODO:
        # Replace it with per key reader writer lock.
        # Current customized lock is just for generic lock not key specific.
        # Ideally it should take key as input and do locking key wise

        lock.acquire_read()
        value = cache.get(key)

        if value:
            lock.release_read()
            return {"status": "OK", "value": value}

        value = persistent.get(key)

        if value:
            lock.release_read()
            return {"status": "OK", "value": value}

        lock.release_read()
        return {"status": "Failed", "message": "No key found"}


    @staticmethod
    def put(key, value, timestamp, cache, persistent, lock):
        lock.acquire_write()
        data = cache.get(key)
        revoked_values = []
        if data:
            current_value, current_timestamp = data
            if timestamp > current_timestamp:
                revoked_values.extend(cache.put(key, (value, timestamp)))
        else:
            revoked_values.extend(cache.put(key, (value, timestamp)))

        for key, value in revoked_values:
            persistent.put(key, value)

        lock.release_write()

        return {"status": "OK"}


    @staticmethod
    def get_timestamp(key, cache, persistent, lock):
        lock.acquire_read()
        data = cache.get(key)
        if not data:
            data = persistent.get(key)

        if data:
            current_value, current_timestamp = data
            lock.release_read()

            return {"status": "OK", "timestamp": current_timestamp}

        lock.release_read()

        return {"status": "Failed", "message": "No key"}
