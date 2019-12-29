class ProtocolInterface(object):

    def get_timestamp(self, key, server_list):
        raise NotImplementedError

    def put(self, key, value, server_list=None, insert=False):
        raise NotImplementedError

    def get(self, key, server_list):
        raise NotImplementedError