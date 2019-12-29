



invoke < get, key >:
    class, server_list = look_up_metadata_in_cache < key >

    # If exist then make a get call based on protocol specified in class
    if class exist:
        value = invoke < class->protocol, get , server_list, key >
        valid = check_validity < value >  # Validy also checks if value doesn't exist
        if valid:
            return value

    # Fetch the metadata from metadata server
    class, server_list = look_up_in_metadata_server < key >

    if class exist:
        invoke < store_metadata_in_cache, class, server_list >
        value = invoke < class->protocol, get , server_list >
        return value

    return NULL

