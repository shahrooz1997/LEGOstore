# For this protocol we would assume that persistent storage would look like:
# key -> timestamp
# key + timestamp -> (value, label, prev_timestamp)
# Reason for choosing this is even if no garbage collector for persisten storage,
# it can still impact performance at minimal rather than having
# key -> [(value1, label1, timestamp1), (value1, label1, timestamp1), (value1, label1, timestamp1)]
# Confirm this with Professor Bhuvan too.
import threading

class Viveck_1Server:
    
    #version ordererd
    timeorder_log = {}
    
    #history of timestamps
    timestamps_history = {}

    #lock for timstamp history and order
    timestamp_lock = threading.lock()


    @staticmethod
    def record_timestamp_order(key, timestamp, current_timestamps):
        timestamp_lock.acquire()
        order = 0
        for index, _timestamp in enumerate(current_timestamps):
            if _timestamp == timestamp:
                order = index
                break
            
        #find the order counts
        counts = Viveck_1Server.timeorder_log.get(key)
        if (len(counts)-1) < order:
            for _ in range(order - (len(counts)-1)):
                counts.append(0)

        counts[order] += 1
        Viveck_1Server.timeorder_log.update({key:counts})
        timestamp_lock.release()
    @staticmethod
    def insert_timestamp_history(key, timestamp):
        timestamp_lock.acquire()
        # first time in server
        if Viveck_1Server.timestamps_history.get(key) is None:
            Viveck_1Server.timestamps_history.update({key:[timestamp]})
            # Check if key exists in dictionary
            if Viveck_1Server.timeorder_log.get(key) is None:
                Viveck_1Server.timeorder_log.update({key:[0]}) 
        else:
            _timestamps = Viveck_1Server.timestamps_history.get(key)
            # Do not need to check if running expirement
            if timestamp not in _timestamps:
                _timestamps = [timestamp] + _timestamps    
                Viveck_1Server.timestamps_history.update({key:_timestamps})
        timestamp_lock.release()
        return


    @staticmethod
    def get_timestamp(key, cache, persistent, lock):
        lock.acquire_read()
        data = cache.get(key)

        if not data:
            data = persistent.get(key)

        #Redundant code. Get rid of this if here.
        if not data:
            lock.release_read()
            return {"status": "Failed", "timestamp": None}

        if not data[0]:
            lock.release_read()
            return {"status": "Failed", "timestamp": None}
        
#        Viveck_1Server.insert_timestamp_history(key, data[1])
        
        lock.release_read()
        return {"status": "OK", "timestamp": data[1]}


    @staticmethod
    def put(key, value, timestamp, cache, persistent, lock):
        '''
        PUT API call
        :param timestamp:
        :param cache:
        :param persistent:
        :param lock:
        :return:
        '''
        lock.acquire_write()
        Viveck_1Server.insert_data(key, value, timestamp, False, cache, persistent)
        lock.release_write()
        Viveck_1Server.insert_timestamp_history(key,timestamp)
        return {"status": "OK"}


    @staticmethod
    def insert_data(key, value, timestamp, label, cache, persistent):
        # Verify once more if anyone else has insterted the key value in between
        if cache.get(key+timestamp):
            return

        current_storage = cache
        data = cache.get(key)
        if not data:
            data = persistent.get(key)
            current_storage = persistent
    
        if not data[0]:
            if label:
                fin_timestamp = timestamp
            else:
                fin_timestamp = None

            new_values = [(key, [timestamp, fin_timestamp]), (key+timestamp, [value, label, None])]

            cache_thread = threading.Thread(target=cache.put_in_batch, args=(new_values,))
            persistent_thread = threading.Thread(target=persistent.put_in_batch, args=(new_values,))

            cache_thread.start()
            persistent_thread.start()

            cache_thread.join()
            persistent_thread.join()

            return

        # Step1 : Insert into the begining if its most recent
        current_final_timestamp = data[0]
        current_timestamp = data[0]
        current_fin = data[1]
        if timestamp > current_timestamp:
            if label:
                current_fin = timestamp
            new_values = [(key, [timestamp, current_fin]),
                          (key+timestamp, [value, label, current_timestamp])]

            cache_thread = threading.Thread(target=cache.put_in_batch, args=(new_values,))
            persistent_thread = threading.Thread(target=persistent.put_in_batch, args=(new_values,))

            cache_thread.start()
            persistent_thread.start()

            cache_thread.join()
            persistent_thread.join()

            return

        while current_timestamp:
            data = current_storage.get(key+current_timestamp)

            if not data:
                current_storage = persistent
                data = current_storage.get(key+current_timestamp)

            curr_value, curr_label, next_timestamp = data

            if next_timestamp == None or next_timestamp < timestamp:
                new_values = [(key+current_timestamp, [curr_value, curr_label, timestamp]),
                              (key+timestamp, [value, label, next_timestamp])]

                # Changing the fin timestamp to timestamp
                if label and (not current_fin or current_fin < timestamp):
                    new_values.append((key, [current_final_timestamp, timestamp]))

                cache_thread = threading.Thread(target=cache.put_in_batch, args=(new_values,))
                persistent_thread = threading.Thread(target=persistent.put_in_batch, args=(new_values,))

                cache_thread.start()
                persistent_thread.start()

                cache_thread.join()
                persistent_thread.join()

                return

            current_timestamp = next_timestamp

        raise Exception("Buggy code as it should never reach here!!!")


    @staticmethod
    def put_fin(key, timestamp, cache, persistent, lock):
        # Using optimistic approach i.e. we assume that it will be in cache else we have to
        lock.acquire_write()
        data = cache.get(key+timestamp)
        if not data:
            data = persistent.get(key+timestamp)

        if not data[0]:
            Viveck_1Server.insert_data(key, None, timestamp, True, cache, persistent)
            lock.release_write()
            return {"status": "OK"}
        else:
            current_values = cache.get(key)
            if not current_values:
                current_values = persistent.get(key)

            current_timestamp, current_fin_timestamp = current_values

            data_values = [(key+timestamp, [data[0], True, data[2]])]

            if not current_fin_timestamp or current_fin_timestamp <= timestamp:
                data_values.append((key, [current_timestamp, timestamp]))

            cache_thread = threading.Thread(target=cache.put_in_batch, args=(data_values,))
            persistent_thread = threading.Thread(target=persistent.put_in_batch, args=(data_values,))

            cache_thread.start()
            persistent_thread.start()

            cache_thread.join()
            persistent_thread.join()

            lock.release_write()
            return {"status": "OK"}


    @staticmethod
    def put_back_in_cache(key, timestamp, data, cache):
        cache.put(key+timestamp, data)
        return


    @staticmethod
    def get(key, timestamp, cache, persistent, lock, required_value):
        # Replace it with per key reader writer lock.
        # Current customized lock is just for generic lock not key specific.
        # Ideally it should take key as input and do locking key wise
        
        Viveck_1Server.record_timestamp_order(key,timestamp\
                        , Viveck_1Server.timestamps_history.get(key))
        
        lock.acquire_read()
        data = cache.get(key+timestamp)
        if not data:
            data = persistent.get(key+timestamp)
            lock.release_read()

            if len(data) == 1:
                lock.acquire_write()
                Viveck_1Server.insert_data(key, None, timestamp, True, cache, persistent)
                lock.release_write()
                return {"status": "OK", "value": "None"}

            lock.acquire_write()
            Viveck_1Server.put_back_in_cache(key, timestamp, data, cache)
            lock.release_write()

        lock.release_read()
        if required_value:
            return {"status": "OK", "value": data[0]}

        return {"status": "OK", "value": "None"}
