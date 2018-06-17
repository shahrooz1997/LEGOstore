# For this protocol we would assume that persistent storage would look like:
# key -> timestamp
# key + timestamp -> (value, label, prev_timestamp)
# Reason for choosing this is even if no garbage collector for persisten storage,
# it can still impact performance at minimal rather than having
# key -> [(value1, label1, timestamp1), (value1, label1, timestamp1), (value1, label1, timestamp1)]
# Confirm this with Professor Bhuvan too.

class Viveck_1Server:

    @staticmethod
    def get_latest_fin(values):
        # TODO: We can also store the index of the first fin timestamp if exist in list
        # TODO: This is the advice from Professor Viveck.
        if not values:
            return None

        for data in values:
            if not data:
                continue
            value, timestamp, label = data
            if label:
                return timestamp

        return None


    @staticmethod
    def persist_on_disk(data_list, persistent):
        '''
        This is the generic method which inserts key, value in the disk
        :param key:
        :param value:
        :param timestamp:
        :param cache:
        :param persistent:
        :return:
        '''
	
        for key, value_list in data_list:
            current_timestamp = persistent.get(key)[0]

            if not current_timestamp:
                current_timestamp = None

            for value, timestamp, label in reversed(value_list):
                print(value)
                print(timestamp)
                print(label)
                persistent.put(key+timestamp, [value, label, current_timestamp])
                current_timestamp = timestamp


            persistent.put(key, [current_timestamp])

        return


    @staticmethod
    def get_timestamp(key, cache, persistent, lock):
        lock.acquire_read()
        data = cache.get(key)
        timestamp = None

        if data:
            print(data)
            timestamp = Viveck_1Server.get_latest_fin(data)

        if timestamp:
            lock.release_read()
            return {"status": "OK", "timestamp": timestamp}

        timestamp = persistent.get(key)[0]

        final_timestamp = None
        # This part of code hits the disk again and again till reached the required timestamp
        # Confirm with Professor if he has alternate in mind
        if timestamp:
            while timestamp:
                data = persistent.get(key+timestamp)
                #print("data is ==============================> : " + str(type(data)) + "  => " + persistent.get(key+timestamp))
                value, label, prev_timestamp = persistent.get(key+timestamp)

                if label == True:
                    final_timestamp = timestamp
                    break

                timestamp = prev_timestamp

        lock.release_read()

        if timestamp:
            return {"status": "OK", "timestamp": final_timestamp}

        return {"status": "Failed", "message": "No key"}


    @staticmethod
    def update_with_new_value(key, value, timestamp, cache, persistent):
        # If the key exist in the cache or new entry it will be updated in the cache
        # Else it will return false
        data = cache.get(key)
        if not data:
            return False
        current_value, current_timestamp, label = data[0]

        # If new timestamp is greater than the recent one in cache
        # It implies that it is a new key and should be inserted
        if timestamp > current_timestamp:
            # TODO: Take care of discarded value
            evicted_values = cache.put(key, [[value, timestamp, False]] + data)
            if evicted_values:
                Viveck_1Server.persist_on_disk(evicted_values, persistent)
            return True

        # If timestamp is lesser than the least recent key
        # It implies that it might also be present in persisten storage so better look there.
        last_cache_value, last_cache_timestamp, last_label = data[-1]
        if timestamp < last_cache_timestamp:
            return False

        for current_data in data:
            if current_data[1] == current_timestamp and current_data[2]:
                # TODO: verify it it works! I.e. pass by reference works here or not
                current_data[0] = value
                return True
            elif current_data[1] == current_timestamp:
                # Should never ever ever happen!!!!
                raise Exception("Buggy code! We got timestamp existing timestamp without fin tag")

        return False


    @staticmethod
    def insert_in_cache(key, value, label, timestamp, cache, persistent):
        values = cache.get(key)

        if not values:
            evicted_values = cache.put(key, [[value, timestamp, label]])
            if evicted_values:
                Viveck_1Server.persist_on_disk(evicted_values, persistent)
            return

        for index, current_value in enumerate(values):
            if timestamp > current_value[1]:
                new_values = values[0:index] + [[value, timestamp, label]] + [values[index:]]
                evicted_values = cache.put(key, new_values)
               
                if evicted_values:
                     Viveck_1Server.persist_on_disk(evicted_values, persistent)
                return

        evicted_values = cache.put(key, values + [[value, timestamp, label]])
        
        if evicted_values:
            Viveck_1Server.persist_on_disk(evicted_values, persistent)
        
        return


    @staticmethod
    def put(key, value, timestamp, cache, persistent, lock):
        # Using optimistic approach i.e. we assume that it will be in cache else we have to
        lock.acquire_write()
        print("reached here vivieck ==========================================")
        result = Viveck_1Server.update_with_new_value(key, value, timestamp, cache, persistent)
        if result:
            lock.release_write()
            return {"status": "OK"}

        current_persisted_timestamp = persistent.get(key)[0]
        current_timestamp = current_persisted_timestamp

        if not current_timestamp:
            Viveck_1Server.insert_in_cache(key, value, False, timestamp, cache, persistent)
            lock.release_write()
            return {"status": "OK"}

        # Little complex code to get it from disk again and again and see
        if current_timestamp < timestamp:
            persistent.put(key+timestamp, [value, False, current_timestamp])
            persistent.put(key, [timestamp])
            # TODO: Should be do sync here too
            lock.release_write()
            return {"status": "OK"}

        # Case1 : When this is going to be the latest timestamp on disk
        # Case2 : When it is going to be inserted in between
        # Case3 : When it is going to be inserted in the end
        while current_timestamp:
            curr_value,  label, next_timestamp = persistent.get(key + current_timestamp)

            # Same timestamp that means updating the value
            if current_timestamp == timestamp:
                # Sanity check that if found existing timestamp it should be labeled as True
                #assert(label, True)
                persistent.put(key+timestamp, [curr_value, label, next_timestamp])
                break
            elif next_timestamp == None or next_timestamp < timestamp:
                persistent.put(key+current_timestamp, [curr_value, label, timestamp])
                persistent.put(key+timestamp, [value, False, next_timestamp])
                break

        lock.release_write()
        return {"status": "OK"}


    @staticmethod
    def update_with_new_label(key, timestamp, cache, persistent):
        data = cache.get(key)
        #print(data)
        if not data:
            return (None, False)
        
        print("data ::" + str(data))
        current_value, current_timestamp, label = data[0]

        # If new timestamp is greater than the recent one in cache
        # It implies that it is a new key and should be inserted
        if timestamp > current_timestamp:
            # TODO: Take care of discarded value
            evicted_values = cache.put(key, [[None, timestamp, True]] + data)
            if evicted_values:
                Viveck_1Server.persist_on_disk(evicted_values, persistent)

            return (None, True)

        # If timestamp is lesser than the least recent key
        # It implies that it might also be present in persisten storage so better look there.
        last_cache_value, last_cache_timestamp, last_label = data[-1]
        if timestamp < last_cache_timestamp:
            return (None, False)

        for current_data in data:
            if current_data[1] == current_timestamp:
                # TODO: verify it it works! I.e. pass by reference works here or not
                current_data[2] = True
                return (current_data[0], True)

        return (None, False)


    @staticmethod
    def put_fin(key, timestamp, cache, persistent, lock):
        # Using optimistic approach i.e. we assume that it will be in cache else we have to
        lock.acquire_write()
        value, found = Viveck_1Server.update_with_new_label(key, timestamp, cache, persistent)

        if found:
            lock.release_write()
            return {"status": "OK"}

        current_persisted_timestamp = persistent.get(key)[0]
        current_timestamp = current_persisted_timestamp

        if not current_timestamp:
            Viveck_1Server.insert_in_cache(key, None, True, timestamp, cache, persistent)
            lock.release_write()
            return {"status": "OK"}

        # Little complex code to get it from disk again and again and see
        if current_timestamp < timestamp:
            persistent.put(key+timestamp, [None, False, current_timestamp])
            persistent.put(key, [timestamp])
            # TODO: Should we do sync here too??
            lock.release_write()
            return {"status": "OK"}

        # Case1 : When this is going to be the latest timestamp on disk
        # Case2 : When it is going to be inserted in between
        # Case3 : When it is going to be inserted in the end
        while current_timestamp:
            curr_value,  label, next_timestamp = persistent.get(key + current_timestamp)

            # Same timestamp that means updating the value
            if current_timestamp == timestamp:
                # Sanity check that if found existing timestamp it should be labeled as True
                #assert(label, True)
                persistent.put(key+timestamp, [curr_value, True, next_timestamp])
                break

            elif next_timestamp == None or next_timestamp < timestamp:
                persistent.put(key+current_timestamp, [curr_value, label, timestamp])
                persistent.put(key+timestamp, [None, True, next_timestamp])
                break

            current_timestamp = next_timestamp

        lock.release_write()

        return {"status": "OK"}

    @staticmethod
    def get(key, timestamp, cache, persistent, lock):
        # Replace it with per key reader writer lock.
        # Current customized lock is just for generic lock not key specific.
        # Ideally it should take key as input and do locking key wise

        lock.acquire_write()
        value, found = Viveck_1Server.update_with_new_label(key, timestamp, cache, persistent)

        if found:
            lock.release_write()
            return {"status": "OK", "value": value}


        values = persistent.get(key+timestamp)
        if values:
            value, label, next_timestamp = values

            # Here is a trick! Now we have assumed that key values are always sorted
            # In case anyone has called any older key, we will only put most recent key in-memory back

            # Step1: First persist it on disk with new label
            if not label:
                persistent.put(key+timestamp, [value, True, next_timestamp])
                Viveck_1Server.insert_in_cache(key, value, label, timestamp, cache, persistent)

            # Step2: Get the most recent version in cache
            cache_values = cache.get(key)
            if not cache_values:
                recent_timestamp_on_disk = persistent.get(key)[0]
                data = persistent.get(key+timestamp)
                Viveck_1Server.insert_in_cache(key, data[0], data[1], recent_timestamp_on_disk, cache, persistent)

            lock.release_write()
            return {"status": "OK", "value": value}
        else:
            current_persisted_timestamp = persistent.get(key)[0]
            current_timestamp = current_persisted_timestamp

            if not current_timestamp:
                Viveck_1Server.insert_in_cache(key, None, True, timestamp, cache, persistent)
                lock.release_write()
                return {"status": "OK", "value": None}

            # Little complex code to get it from disk again and again and see
            if current_timestamp < timestamp:
                persistent.put(key + timestamp, [None, True, current_timestamp])
                persistent.put(key, [timestamp])
                # TODO: Should be do sync here too
                lock.release_write()
                return {"status": "OK", "value": None}

            # Case1 : When this is going to be the latest timestamp on disk
            # Case2 : When it is going to be inserted in between
            # Case3 : When it is going to be inserted in the end
            while current_timestamp:
                curr_value, label, next_timestamp = persistent.get(key + current_timestamp)

                if next_timestamp == None or next_timestamp < timestamp:
                    persistent.put(key + current_timestamp, [curr_value, label, timestamp])
                    persistent.put(key + timestamp, [None, True, next_timestamp])
                    break

                current_timestamp = next_timestamp

        lock.release_write()
        return {"status": "OK", "value": None}

