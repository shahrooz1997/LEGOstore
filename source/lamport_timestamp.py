import datetime

class TimeStamp:
    @staticmethod
    def get_current_time(_id):
        return str(datetime.datetime.utcnow()) + '-' + _id


    @staticmethod
    def get_next_timestamp(timestamp_list, _id):
        timestamps = []
        for timestamp in timestamp_list:
            timestamps.append(timestamp.split("-")[0])

        next_timestamp = max(max(timestamps), str(datetime.datetime.utcnow())) + '-' + _id

        return next_timestamp


    @staticmethod
    def get_max_timestamp(timestamp_list):
        return max(timestamp_list)


    @staticmethod
    def compare_time(time1, time2):
        timestamp_1, id_1 = time1.split("-")
        timestamp_2, id_2 = time2.split("-")

        if timestamp_1 == timestamp_2:
            return id_1 > id_2

        return timestamp_1 > timestamp_2
