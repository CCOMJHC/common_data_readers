import calendar
import logging
import os
import struct
import time

import numpy as np
from pathlib import Path
from hyo2.openbst.lib.raw.parsers.reson.dg_formats import parse, ResonDatagrams, reson_datagram_code

logger = logging.getLogger(__name__)


class Reson:

    def __init__(self, input_path: Path):
        # Object attributes
        self._valid = False
        self.data = None
        self.map = None
        self.mapped = False
        self.file = None

        # File attributes
        self._header_size = 64  # TODO: Verify if this is proper use of leading '_'. These are special private variables
        self._header_format = '<2H4I2Hf2BH2I2HI2H3I'
        self._footer_size = 4
        self._reson_sync_patt = 65535  # Integer representation of 0x0000FFFF, Reson Sync Pattern
        self.format_type = None
        self.file_length = None
        self.file_location = None
        self.file_end = False

        # Call initializing methods
        self.check_file(input_path)

    @property
    def valid(self):
        return self._valid

    # Initializing Methods
    def check_file(self, file_path: Path):
        path_parts = file_path.parts
        self.format_type = path_parts[-1].split('.')[-1]
        valid_formats = ['s7k', ]
        try:                            # TODO: Write warning for 7k type and skip processing file
            if self.format_type in valid_formats:
                self.file = file_path.open(mode='rb')
                self.file.seek(0)
                self.file_length = os.stat(self.file.name).st_size
                self.file_location = self.file.tell()
                self._valid = True
            else:
                logger.error("Unexpected format type: %s" % self.format_type)
                self._valid = False
        except FileNotFoundError:
            logger.error("File Extension Invalid: Currently supporting 's7k' only.")
            self._valid = False
        return self._valid

    def data_map(self, force=False):
        if self.mapped is True or force is True:
            dg_map = self.map
            return dg_map

        if self.file_location != 0:
            self.file.seek(0)

        self.file_end = False
        dg_map = dict()

        while not self.file_end:
            header, count = self.read_dg_header()
            if count != 0:
                logging.warning("Sync pattern misalignment, file pointer shifted %d Bytes forward" % count)

            if header is None:
                self.file_end = True
                break

            dg_type = header[12]
            dg_opd_offset = header[4]
            dg_year = header[6]
            dg_day = header[7]
            dg_second = header[8]
            dg_hour = header[9]
            dg_minute = header[10]
            dg_time = self.get_time(dg_year, dg_day, dg_hour, dg_minute, dg_second)

            dg_data_header_loc = self.file_location
            dg_data_size = header[3] - self._header_size - self._footer_size

            map_data_entry = [dg_data_header_loc, dg_time, dg_data_size, dg_opd_offset]

            if dg_type in dg_map:
                dg_map[dg_type].append(map_data_entry)
            else:
                dg_map[dg_type] = list()
                dg_map[dg_type].append(map_data_entry)

        self.map = dg_map
        self.mapped = True
        return dg_map

    def is_mapped(self):
        if not self.mapped:
            self.data_map()

    # Datagram Reading Methods
    def read_dg_header(self, count=0):
        chunk = self.file.read(self._header_size)
        self.file_location = self.file.tell()
        if len(chunk) != self._header_size:
            logging.debug("End of file")
            header_data = None
            return header_data, count

        header_data = struct.unpack(self._header_format, chunk)

        header_sync_patt = header_data[2]
        if header_sync_patt != self._reson_sync_patt:   # TODO: This is a recursive data check. It can be simpler
            self.file.seek(-(self._header_size-1), 1)
            header_data, count = self.read_dg_header()
            return header_data, count + 1
        else:
            data_size = header_data[3]
            self.file.seek(data_size - self._header_size, 1)
            return header_data, count

    def get_datagram(self, dg_type: ResonDatagrams, dg_record_range=None, dg_time=None):
        self.is_mapped()
        dg_code = reson_datagram_code[dg_type]
        dg_num_records = len(self.map[dg_code])
        data_out = list()

        # Determine indices of all desired datagrams
        map_index = None
        if dg_record_range is None:
            map_index = range(dg_num_records)  # Get all the data records
        elif dg_record_range is not None and dg_time is None:
            if any(dg_record_number > dg_num_records for dg_record_number in dg_record_range):
                raise RuntimeError("Index %d exceeds number of datagram entries (%d)"
                                   % (dg_record_range, dg_num_records))
            map_index = dg_record_range            # Get Records indicated by range
        elif dg_record_range is None and dg_time is not None:
            pass  # get records in time range
        else:
            pass  # get specified records and time range

        # loop over indices and place data in
        for n in map_index:
            dg_data_header_loc = self.map[dg_code][n][0]
            dg_time = self.map[dg_code][n][1]
            dg_size = self.map[dg_code][n][2]

            datapacket = self.get_record(dg_type, dg_data_header_loc, dg_size)
            datapacket.time = dg_time
            data_out.append(datapacket)

        return data_out

    def get_record(self, dg_type, dg_data_header_loc, dg_size):
        self.file.seek(dg_data_header_loc, 0)  # go to file position
        dg_chunk = self.file.read(dg_size)  # extract the data
        datapacket = parse(dg_chunk, dg_type)  # Parse the data

        return datapacket

    @staticmethod
    def get_time(year, day, hour, minute, second):                  # TODO: Change this to use the datetime modules
        time_fmt = '%Y, %j, %H, %M'
        temp_string = '%d, %d, %d, %d' % (year, day, hour, minute)
        timestruct = time.strptime(temp_string, time_fmt)
        utctime = calendar.timegm(timestruct) + second

        return utctime * 1000        # Time in milliseconds to adhere to the cf standard
