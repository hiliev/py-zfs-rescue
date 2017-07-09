from io import BytesIO
import struct

# nvpair data types
DATA_TYPE_UNKNOWN = 0
DATA_TYPE_BOOLEAN = 1
DATA_TYPE_BYTE = 2
DATA_TYPE_INT16 = 3
DATA_TYPE_UINT16 = 4
DATA_TYPE_INT32 = 5
DATA_TYPE_UINT32 = 6
DATA_TYPE_INT64 = 7
DATA_TYPE_UINT64 = 8
DATA_TYPE_STRING = 9
DATA_TYPE_BYTE_ARRAY = 10
DATA_TYPE_INT16_ARRAY = 11
DATA_TYPE_UINT16_ARRAY = 12
DATA_TYPE_INT32_ARRAY = 13
DATA_TYPE_UINT32_ARRAY = 14
DATA_TYPE_INT64_ARRAY = 15
DATA_TYPE_UINT64_ARRAY = 16
DATA_TYPE_STRING_ARRAY = 17
DATA_TYPE_HRTIME = 18
DATA_TYPE_NVLIST = 19
DATA_TYPE_NVLIST_ARRAY = 20
DATA_TYPE_BOOLEAN_VALUE = 21
DATA_TYPE_INT8 = 22
DATA_TYPE_UINT8 = 23
DATA_TYPE_BOOLEAN_ARRAY = 24
DATA_TYPE_INT8_ARRAY = 25
DATA_TYPE_UINT8_ARRAY = 26
DATA_TYPE_DOUBLE = 27


class TypedBytesIO(BytesIO):

    def __init__(self, data):
        super().__init__(data)

    def read_uint32(self):
        data = self.read(4)
        return struct.unpack(">L", data)[0]

    def read_uint64(self):
        data = self.read(8)
        return struct.unpack(">Q", data)[0]

    def read_string(self):
        str_len = self.read_uint32()
        data_size = self._align4(str_len)
        data = self.read(data_size)
        return data[:str_len].decode("ascii"), data_size + 4

    @staticmethod
    def _align4(size):
        """
        Adjust the size to a multiple of 4

        :param size: object size
        :return: nearest multiple of 4 no less than the size
        """
        return (size + 3) & ~3


class NVPairParser:

    def __init__(self):
        pass

    def parse(self, data):
        stream = TypedBytesIO(data)
        return self._parse(stream)

    def _parse(self, data_stream):
        nvlist = {}

        # Read header
        header = data_stream.read(8)
        if (len(header) != 8) and (header[-1] != 1):
            raise Exception("Bad nvpair header")

        while True:
            # print("Reading pair...")
            # Get nvpair packed size
            size = data_stream.read_uint32()
            # Skip nvpair unpacked size
            data_stream.read_uint32()
            if size == 0:
                break
            # Get name
            name, name_len = data_stream.read_string()
            # Get datatype
            data_type = data_stream.read_uint32()
            # Get item count
            item_count = data_stream.read_uint32()
            value = []
            for i in range(item_count):
                # Read value
                if data_type == DATA_TYPE_UINT32:
                    v = data_stream.read_uint32()
                elif data_type == DATA_TYPE_UINT64:
                    v = data_stream.read_uint64()
                elif data_type == DATA_TYPE_STRING:
                    v = data_stream.read_string()[0]
                elif data_type == DATA_TYPE_NVLIST:
                    v = self._parse(data_stream)
                elif data_type == DATA_TYPE_NVLIST_ARRAY:
                    v = self._parse(data_stream)
                else:
                    v = data_stream.read(size - (16+name_len))
                value.append(v)
            if item_count == 1:
                value = value[0]
            nvlist[name] = value
        return nvlist
