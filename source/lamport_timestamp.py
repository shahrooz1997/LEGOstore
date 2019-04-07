import datetime

class Timestamp:
    @staticmethod
    def create_new_timestamp(_id):
        assert(type(_id) == str)
        return str(0)+'-'+_id


    @staticmethod
    def increment_timestamp(timestamp, _id):
        assert(len(timestamp.split('-')) == 2)
        assert(type(_id) is str)        
        _time_plus_one = int(timestamp.split('-')[0]) + 1
        next_timestamp = str(_time_plus_one) + '-' + _id
        return next_timestamp 


    @staticmethod
    def get_max_timestamp(timestamp_list):
        print(" >> get_max_timestamp >> timestamp_list = ", timestamp_list)
        max_timestamp = None
        max_time = None
        for timestamp in timestamp_list:
            if timestamp is not None:
                time = int(timestamp.split('-')[0])
            else:
                continue
            src_client = int(timestamp.split('-')[1])
            if max_timestamp is None or time > max_time:
                max_time = time
                max_timestamp = "{}-{}".format(max_time, src_client)
        assert(max_timestamp is not None)
        return max_timestamp



if __name__ == '__main__':

    #TODO: use unittest instead
    print("Testing Timestamp")
    id_1 =  '1'
    id_2 =  '2'
    id_3 =  '3'
    timestamp_1 = Timestamp.create_new_timestamp(id_1)
    timestamp_2 = Timestamp.create_new_timestamp(id_2)
    timestamp_2 = Timestamp.increment_timestamp(timestamp_2, id_2) 
    print("current timestamp_1\t--> " , timestamp_1)
    print("current timestamp_2\t--> " , timestamp_2)
    max_timestamp = Timestamp.get_max_timestamp([timestamp_1, timestamp_2])
    print("max timestamp\t-->", max_timestamp)
    next_timestamp = Timestamp.increment_timestamp(max_timestamp, id_3)
    print("next timestamp\t-->", next_timestamp)
    print("done testing Timestamp")
    
