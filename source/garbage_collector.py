import time

# TODO: Not happy with current design , modify it to be something better soon.
# TODO: Have a way to evict last i.e. update it on the server and marked evict possible on the cache
# Here we have something like : key+timestamp->(value, label, prev_timestmap)

def garbage_collector(lock, cache, persistent, allowed_in_cache = 1):
    '''
    It will read all the keys from the cache (without changing there order).
    In a batch of 1000 keys it will try to dump it on disk while updating the cache (again without changing order).
    Note since all the keys are read once it is possible that few keys no longer exist on cache.
    Garbage collector simply ingore those keys
    After every 1000 keys it will release the lock and will try to get the lock again.

    :param lock:
    :param cache:
    :param persistent:
    :return:

    '''
    print("garbage_collector_called")
    lock.acquire_read()

    # We will assume that keys of small size and all can be stored in single variable
    current_keys = cache.keys()
    lock.release_read()

    # Lets go first with 1000 keys at a time
    count = 0
    persist_value = {}
    cache_value = {}

    for key in current_keys:
        if count % 1000 == 0:
            lock.acquire_write()
            acquired = True
            cache_value = {}

        values = cache.get_without_modifying(key)

        # TODO: For now its just the one value in cache. Rest all sits on disk
        if values and len(values) > allowed_in_cache:
            cache_value[key] = [values.pop(0)]
        else:
            continue

        # Storing current timestamp of persistent storage and
        # Updating the new current timestamp
        current_timestamp = persistent.get(key)[0]

        persistent.put(key, [values[0][1]])

        for index, value in enumerate(values):
            if index < len(values) - 1:
                persistent.put(key+value[1],  [value[0], value[2], values[index+1][1]])
            else:
                persistent.put(key+value[1], [value[0], value[2], current_timestamp])

        if count % 1000 == 999:
            # It will make sure that even if the key isn't there in cache it won't update it.
            # It will assume that it is revoked and stored in presist medium and we don't have to worry about it
            cache.update_without_modifying(cache_value)
            if acquired:
                acquired = False

            lock.release_write()

        # TODO: Implement how it will work for rocksdb
        # Although with current implementation it should work easily
        count += 1

    if acquired:
       if cache_value:
           cache.update_without_modifying(cache_value)
       lock.release_write()
