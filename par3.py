"""
par.py
G.Rice 6/20/2012
Updated by E.Younkin 12/13/2019

This is free and unencumbered software released into the public domain.

Anyone is free to copy, modify, publish, use, compile, sell, or
distribute this software, either in source code form or as a compiled
binary, for any purpose, commercial or non-commercial, and by any
means.

In jurisdictions that recognize copyright laws, the author or authors
of this software dedicate any and all copyright interest in the
software to the public domain. We make this dedication for the benefit
of the public at large and to the detriment of our heirs and
successors. We intend this dedication to be an overt act of
relinquishment in perpetuity of all present and future rights to this
software under copyright law.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

V1.0 20191213
This module includes a number of different classes and methods for working with
Kongsberg all files, each of which are intended to serve at least one of three
purposes.  These are

   1) Provide access to the Kongsberg records for viewing or data extraction.
   2) Provide simplified access to a combination of Kongsberg data.
   3) Display information from Kongsberg records or data.
   
The primary classes in this module to be accessed directly are

    AllRead - used to get data records or blocks of records from a Kongsberg .all file.
    BatchRead - use Xarray and Dask modules to read multiple .all files in parallel.

"""

import sys
import os
import numpy as np
from matplotlib import pyplot as plt
from mpl_toolkits.basemap import pyproj
import datetime as dtm
import pickle
from glob import glob
import struct
import re
import copy
import webbrowser
from time import perf_counter
from itertools import islice
from sortedcontainers import SortedDict
import json

try:
    from dask.distributed import get_client, Client, wait
    import xarray as xr
    batch_read_enabled = True
except ModuleNotFoundError:
    batch_read_enabled = False

recs_categories = {'65': ['data.Time', 'data.Roll', 'data.Pitch', 'data.Heave', 'data.Heading'],
                   '73': ['time', 'settings'],
                   '78': ['time', 'header.SoundSpeed', 'header.Ntx', 'header.Nrx', 'header.Nvalid',
                          'header.SampleRate', 'tx.TransmitSector#', 'tx.TiltAngle', 'tx.SignalLength',
                          'tx.Delay', 'tx.Frequency', 'tx.WaveformID', 'rx.BeamPointingAngle',
                          'rx.TransmitSectorID', 'rx.DetectionInfo', 'rx.QualityFactor', 'rx.TravelTime'],
                   '85': ['time', 'data.Depth', 'data.SoundSpeed'],
                   '110': ['data.Time', 'source_data.Latitude', 'source_data.Longitude',
                           'source_data.AlongTrackVelocity', 'source_data.Altitude']}


class AllRead:
    """
    This is the primary class for working with Kongsberg data all files and
    providing access to the data records.  The concept behind this class is
    that the class is a file, and it lets to move around to different records
    in the file.  The class can contain a map of where all the records are in
    the file, a record or a dictionary of records belonging to a ping, and
    dictionary of navigation and attitude data, or other types of data such as
    runtime parameters.
    
    The current record can be found in a class variable called 'packet'.  This
    contains the "header" information that exists for all records, such as
    time, record type, record size.  The data for the record is contained in a
    data record type specific subpacket type inside of the variable packet
    called 'subpack'.  Each of these are their own classes with their own
    variables and methods for working with their own data.
    
    allRead methods of interest:
        getrecord
        getwatercolumn
        display
        getnav
        plot_navarray
        getruntime
        getsscast
        getping
        
    It is worth noting here that the getrecord method calls the mapfile
    method if a file map does not already exist.  The file map is also an
    allRead class variable called 'map'.  The map class has a number of methods 
    of its own, most notibly the method 'printmap' which displays the records 
    available in file and what percentage of the file they consume.  The labels
    for these records (record number) is listed in this map and can be used as 
    a reference when working from the commandline.
    
    """

    def __init__(self, infilename, start_ptr=0, end_ptr=0, byteswap=False):
        """Make a instance of the allRead class."""
        self.infilename = infilename
        self.byteswap = byteswap
        self.infile = open(infilename, 'rb')
        self.mapped = False
        self.packet_read = False
        self.eof = False
        self.error = False
        self.start_ptr = start_ptr
        self.end_ptr = end_ptr
        self.startbytesearch = self._build_startbytesearch()
        self.at_right_byte = False

        if end_ptr:
            self.filelen = int(self.end_ptr - self.start_ptr)
        else:
            self.infile.seek(-self.start_ptr, 2)
            self.filelen = self.infile.tell()
        self.infile.seek(0, 2)
        self.max_filelen = self.infile.tell()
        self.infile.seek(self.start_ptr, 0)

    def close(self, clean=False):
        """
        Close the file from which the data is being read.
        """
        self.infile.close()
        if clean:
            mapfilename = self.infilename + '.par'
            navfilename = self.infilename + '.nav'
            try:
                os.remove(mapfilename)
                os.remove(navfilename)
            except FileNotFoundError:
                pass

    def __enter__(self):
        """
        Start function for with statement
        Now this will work:
        with allRead(file):
                 ....

        The file will be closed automatically
        """
        return self

    def __exit__(self, *args):
        """
        Exit function for with statement
        """
        self.close()

    def _build_startbytesearch(self):
        """
        Build the regular expression we are going to use to find the next startbyte, if necessary.
        """
        # Possible datagram types as int
        recids = [68, 88, 102, 78, 83, 89, 107, 79, 65, 110, 67, 72, 80, 71, 85, 73, 105, 112, 82, 104, 48, 49, 66, 51]
        # search for startbyte + one of the datagramtypes
        search_exp = b'\x02[' + b'|'.join([struct.pack('B', x) for x in recids]) + b']'
        # sonartype always follows datagramtype, include it to eliminate possible mismatches
        ems = [2040, 710, 712]
        emsrchs = []
        for em in ems:
            sonartype = struct.pack('H', em)
            search_exp += sonartype
            compiled_expr = re.compile(search_exp)
            emsrchs.append(compiled_expr)
        return emsrchs

    def seek_next_startbyte(self):
        """
        Determines if current pointer is at the start of a record.  If not, finds the next valid one.
        """
        # check is to continue on until you find the header pattern, which surrounds the STX byte.  Can't just
        #   search for \x02, regex pattern should be the smallest allowable to be 99.99% certain of start
        while not self.at_right_byte:
            cur_ptr = self.infile.tell()
            if cur_ptr >= self.start_ptr + self.filelen:
                self.eof = True
                return
            # consider start bytes right at the end of the given filelength as valid, even if they extend
            # over to the next chunk
            srchdat = self.infile.read(min(20, (self.start_ptr + self.filelen) - cur_ptr))
            stx_idx = srchdat.find(b'\x02')
            if stx_idx >= 0:
                possible_start = cur_ptr + stx_idx
                self.infile.seek(possible_start)
                datchk = self.infile.read(4)
                for srch in self.startbytesearch:
                    m = srch.search(datchk, 0)
                    if m:
                        self.infile.seek(possible_start - 4)
                        self.at_right_byte = True
                        break

    def read(self):
        """
        Reads the header.
        """
        # if running this without offset/maxlen arguments, don't have to worry about finding STX
        #    otherwise you gotta search for the next one...should only need to do it once
        if not self.at_right_byte:
            self.seek_next_startbyte()

        if self.infile.tell() >= self.start_ptr + self.filelen:
            self.eof = True

        if not self.eof:
            # first element of the header is the packetsize
            if self.byteswap:
                packetsize = 4 + np.fromfile(self.infile, dtype=np.uint32, count=1)[0].newbyteorder()
            else:
                packetsize = 4 + np.fromfile(self.infile, dtype=np.uint32, count=1)[0]
            self.infile.seek(-4, 1)
            # with the max length argument, you want to make sure you get the end record, even if it is outside the
            #    range of your given maximum length.
            if (self.filelen >= self.infile.tell() - self.start_ptr + packetsize) or (self.end_ptr > 0):
                self.packet = Datagram(self.infile.read(packetsize), self.byteswap)
                self.packet_read = True
                if not self.packet.valid:
                    self.error = True
                    print("Record without proper STX or ETX found.")
            else:
                self.eof = True
                self.error = True
                print("Broken packet found at", self.infile.tell())
                print("Final packet size", packetsize)

    def get(self):
        """
        Decodes the data section of the datagram if a packet has been read but
        not decoded.  If excecuted the packet_read flag is set to False.
        """
        if self.packet_read and not self.packet.decoded:
            try:
                self.packet.decode()
            except NotImplementedError as err:
                print(err)
            self.packet_read = False

    def _divide_rec(self):
        """
        Data78 comes in from sequential read by time/ping.  Each ping may have multiple sectors to it which we want
        to treat as separate pings.  Do this by generating a new record for each sector in the ping.
        """
        try:
            rec = self.packet.subpack
        except AttributeError:
            raise ValueError('No data found in packet.subpack for record')
        if type(rec) != Data78:
            return [rec]
        elif rec.header['Ntx'] == 1:
            return [rec]
        else:
            totalrecs = []
            for sec in rec.tx['TransmitSector#']:
                split_rec = copy.copy(rec)
                split_rec.tx = split_rec.tx[sec]
                split_rec.rx = split_rec.rx[np.where(split_rec.rx['TransmitSectorID'] == sec)]

                # ping time equals datagram time plus sector transmit delay
                setattr(split_rec, 'time', split_rec.time + split_rec.tx['Delay'])
                #
                totalrecs.append(split_rec)
            return totalrecs

    def pad_to_dense(self, M):
        """
        Appends the minimal required amount of zeroes at the end of each
        array in the jagged array `M`, such that `M` looses its jagedness.
        """

        maxlen = max(len(r) for r in M)

        Z = np.zeros((len(M), maxlen))
        for enu, row in enumerate(M):
            Z[enu, :len(row)] += row
        return Z

    def sequential_read_records(self):
        """
        Using global recs_categories, parse out only the given datagram types by reading headers and decoding only
        the necessary datagrams.

        """
        # recs_to_read is the returned dict of records parsed from the file
        recs_to_read = dict([(k, dict([(r.split('.')[-1].lower(), None) for r in recs_categories[k]])) for k in
                             recs_categories])
        recs_count = dict([(k, 0) for k in recs_categories])

        self.infile.seek(self.start_ptr)
        self.eof = False
        while not self.eof:
            self.read()  # find the start of the record and read the header
            datagram_type = str(self.packet.dtype)
            if datagram_type in list(recs_categories.keys()):  # if the header indicates this is a record you want...
                recs_count[datagram_type] += 1
                self.get()  # read the rest of the datagram and decode the data
                recs = self._divide_rec()  # split up the Data78 if applicable, otherwise just get the same rec back
                for rec in recs:
                    for subrec in recs_categories[datagram_type]:
                        #  override for nested recs, designated with periods in the recs_to_read dict
                        if subrec.find('.') > 0:
                            tmprec = getattr(rec, subrec.split('.')[0])
                            subrec = subrec.split('.')[1]
                        else:
                            tmprec = rec

                        if subrec == 'settings':
                            val = [getattr(tmprec, subrec)]
                        else:
                            try:  # flow for array/list attribute
                                val = list(getattr(tmprec, subrec))
                            except TypeError:  # flow for float/int attribute
                                val = [getattr(tmprec, subrec)]
                            except AttributeError:  # flow for nested recs
                                val = [tmprec[subrec]]

                        # generate new list or append to list for each rec of that dgram type found
                        if recs_to_read[datagram_type][subrec.lower()] is None:
                            recs_to_read[datagram_type][subrec.lower()] = val
                        else:
                            recs_to_read[datagram_type][subrec.lower()].extend(val)

        # these loops are to deal with the lists generated above
        for rec in recs_to_read:
            for dgram in recs_to_read[rec]:
                if recs_count[rec] == 0:
                    recs_to_read[rec][dgram] = np.zeros(0)  # found no records, empty array
                elif rec in ['65', '110']:  # these recs have time blocks of data in them, need to be concatenated
                    recs_to_read[rec][dgram] = np.concatenate(recs_to_read[rec][dgram])
                elif rec == '78':
                    if dgram in ['beampointingangle', 'transmitsectorid', 'detectioninfo', 'qualityfactor',
                                 'traveltime']:
                        # these datagrams can vary in number of beams, have to pad with zeros for 'jaggedness'
                        recs_to_read[rec][dgram] = self.pad_to_dense(recs_to_read[rec][dgram])
                    else:
                        recs_to_read[rec][dgram] = np.array(recs_to_read[rec][dgram])
                else:
                    recs_to_read[rec][dgram] = np.array(recs_to_read[rec][dgram])

        return recs_to_read

    def mapfile(self, verbose=False, show_progress=True):
        """
        Maps the datagrams in the file.
        """
        progress = 0
        if not self.mapped:
            self.map = mappack()
            self.reset()
            if show_progress:
                print('Mapping file;           ', end=' ')
            while not self.eof:
                loc = self.infile.tell()
                self.read()
                dtype = self.packet.header[2]
                dsize = self.packet.header[0]
                time = self.packet.gettime()
                if dtype == 107:
                    try:
                        self.get()
                        pingcounter = self.packet.subpack.header['PingCounter']
                        self.map.add(str(dtype), loc, time, dsize, pingcounter)
                    except:
                        print("Water column record at " + str(loc) + " skipped.")
                else:
                    self.map.add(str(dtype), loc, time, dsize)
                current = 100 * loc / self.filelen
                if current - progress >= 1:
                    progress = current
                    if show_progress:
                        sys.stdout.write('\b\b\b\b\b\b\b\b\b\b%(percent)02d percent' % {'percent': progress})
            self.reset()
            # make map into an array and sort by the time stamp
            self.map.finalize()
            # set the number of watercolumn packets into the map object
            if '107' in self.map.packdir:
                pinglist = list(set(self.map.packdir['107'][:, 3]))
                self.map.numwc = len(pinglist)
            if self.error:
                print()
            else:
                if show_progress:
                    print('\b\b\b\b\b\b\b\b\b\b\b\b finished mapping file.')
            if verbose:
                self.map.printmap()
            self.mapped = True
        else:
            pass

    def loadfilemap(self, mapfilename=''):
        """
        Loads the packdir if the map object packdir has been saved previously.
        """
        if mapfilename == '':
            mapfilename = self.infilename + '.par'
        try:
            self.map = mappack()
            self.map.load(mapfilename)
            self.mapped = True
            print('Loaded file map ' + mapfilename)
        except IOError:
            print(mapfilename + ' map file not found.')

    def savefilemap(self):
        """
        Saves the mappack packdir dictionary for faster operations on a file in
        the future.  The file is saved under the same name as the loaded file
        but with a 'par' extension.
        """
        if self.mapped:
            mapfilename = self.infilename + '.par'
            self.map.save(mapfilename)
            print('file map saved to ' + mapfilename)
        else:
            print('no map to save.')

    def getrecord(self, recordtype, recordnum):
        """
        Gets the record number of the described record type.  The subpacket
        object is returned for easier access to the desired data.
        """
        self.eof = False
        if not self.mapped:
            self.mapfile()
        if str(recordtype) in self.map.packdir:
            loc = int(self.map.packdir[str(recordtype)][recordnum][0])
            # deal with moving within large files
            if loc > 2147483646:
                loc -= 2e9
                self.infile.seek(2e9)
                while loc > 2147483646:
                    loc -= 2e9
                    self.infile.seek(2e9, 1)
                self.infile.seek(loc, 1)
            else:
                self.infile.seek(loc)
            self.read()
            self.get()
            return self.packet.subpack
        else:
            print("record " + str(recordtype) + " not available.")
            return None

    def findpacket(self, recordtype, verbose=False):
        """
        Find the next record of the requested type.
        """
        self.read()
        while not self.eof:
            if verbose:
                print(self.packet.dtype)
            if recordtype == self.packet.dtype:
                break
            else:
                self.read()
        self.get()

    def getwatercolumn(self, recordnum):
        """
        This method is designed to get a watercolumn packet by the ping number
        where ping 0 is the first in the file.  Separate records are
        reassembled for the whole ping and stored as the current subpack class
        as if it were a single record.
        """
        # dt is for looking for packets with different time stamps.
        if not self.mapped:
            self.mapfile()
        pinglist = sorted(set(self.map.packdir['107'][:, 3]))
        if recordnum >= len(pinglist):
            print(str(len(pinglist)) + ' water column records available.')
            return None
        else:
            pingnum = pinglist[recordnum]
            inx = np.nonzero(self.map.packdir['107'][:, 3] == pingnum)[0]
            ping = self.getrecord(107, inx[0])
            numbeams = ping.header['Total#Beams']
            recordsremaining = list(range(ping.header['#OfDatagrams']))
            recordsremaining.pop(ping.header['Datagram#'] - 1)
            totalsamples, subbeams = ping.ampdata.shape
            rx = np.zeros(numbeams, dtype=Data107.nrx_dtype)
            # Initialize array to NANs. Source:http://stackoverflow.com/a/1704853/1982894
            ampdata = np.empty((totalsamples, numbeams), dtype=np.float32)
            ampdata.fill(np.NAN)

            rx[:subbeams] = ping.rx
            ampdata[:, :subbeams] = ping.ampdata
            beamcount = subbeams
            if len(inx) > 1:
                for n in inx[1:]:
                    ping = self.getrecord(107, n)
                    recordnumber = recordsremaining.index(ping.header['Datagram#'] - 1)
                    recordsremaining.pop(recordnumber)
                    numsamples, subbeams = ping.ampdata.shape
                    if numsamples > totalsamples:
                        temp = np.empty((numsamples - totalsamples, numbeams), dtype=np.float32)
                        temp.fill(np.NAN)
                        ampdata = np.append(ampdata, temp, axis=0)
                        totalsamples = numsamples
                    rx[beamcount:beamcount + subbeams] = ping.rx
                    ampdata[:numsamples, beamcount:beamcount + subbeams] = ping.ampdata
                    beamcount += subbeams
            if len(recordsremaining) > 0:
                print("Warning: Not all WC records have the same time stamp!")
            sortidx = np.argsort(rx['BeamPointingAngle'])
            self.packet.subpack.rx = rx[sortidx]
            self.packet.subpack.ampdata = ampdata[:, sortidx]
            self.packet.subpack.header[2] = 1
            self.packet.subpack.header[3] = 1
            self.packet.subpack.header[6] = numbeams
            return self.packet.subpack

    def display(self):
        """
        Prints the current record header and record type header to the command
        window.  If the record type header display method also contains a plot
        function a plot will also be displayed.
        """
        if self.packet_read:
            self.packet.display()
        elif 'packet' in self.__dict__:
            self.packet.display()
            if self.packet.decoded:
                self.packet.subpack.display()
        else:
            print('No record currently read.')

    def reset(self):
        """
        Puts the file pointer to the start and the eof to False.
        """
        self.infile.seek(self.start_ptr)
        self.packet_read = False
        self.eof = False
        if 'packet' in self.__dict__:
            del self.packet

    def getnav(self, tstamps, postype=80, att_type=65, degrees=True):
        """
        For each provided time stamp (single or array) an array
        of navigation data is returned for each of the provided time stamps.
        The returned array set consists of time, x(deg), y(deg), roll (deg), 
        pitch(deg), heave (meters), and heading (deg).  Time stamps are to be
        POSIX time stamps, and are assumed to be in UTC. Set the 'degrees'
        keyword to False have the returned attitude informaiton in radians.
        """
        # make incoming tstamp shape more flexible
        tstamps = np.asarray(tstamps)
        ndim = tstamps.shape
        if len(ndim) == 0:
            tstamps = np.array([tstamps])
        elif len(ndim) == 2:
            tstamps = tstamps[0]
        numpts = len(tstamps)
        # make an array of all the needed data
        if 'navarray' not in self.__dict__:
            self._build_navarray()
        # find bounding times for getting all needed nav data
        if str(att_type) in self.navarray and str(postype) in self.navarray:
            mintime = max(self.navarray[str(att_type)][0, 0], self.navarray[str(postype)][0, 0])
            maxtime = min(self.navarray[str(att_type)][-1, 0], self.navarray[str(postype)][-1, 0])
            navpts = np.zeros((numpts, 7))
            # look for time stamps in the time range
            idx_range = np.nonzero((tstamps <= maxtime) & (tstamps >= mintime))[0]
            if len(idx_range) > 0:
                pos = self.navarray[str(postype)]
                att = self.navarray[str(att_type)]
                # for time stamps in the time range, find that nav and att
                for i in idx_range:
                    ts = tstamps[i]
                    if pos[0, 0] < ts < pos[-1, 0]:
                        prev = np.nonzero(pos[:, 0] <= ts)[0][-1]
                        navpts[i, :3] = self._interp_points(tstamps[i], pos[prev, :], pos[prev + 1, :])
                    else:
                        navpts[i, :3] = np.nan
                    if att[0, 0] < ts < att[-1, 0]:
                        prev = np.nonzero(att[:, 0] <= tstamps[i])[0][-1]
                        navpts[i, 3:] = self._interp_points(tstamps[i], att[prev, :], att[prev + 1, :])[1:]
                    else:
                        navpts[i, 3:] = np.nan
            # convert roll(3), pitch(4) and heading(6) into radians 
            if not degrees:
                navpts[:, [3, 4, 6]] = np.deg2rad(navpts[:, [3, 4, 6]])
            return navpts

    def _interp_points(self, tstamp, pt1, pt2):
        """
        Performs an interpolation for the points given and returns the
        interpolated points.  The first field of each point array is assumed to
        be the time stamp, and all other values in the array are interpolated.
        """
        delta = pt2 - pt1
        result = pt1 + (tstamp - pt1[0]) * delta / delta[0]
        return result

    def _build_navarray(self, allrecords=False):
        """
        The objective is to do the work of building an array of the navigation
        data to speed up processing later.  It is stored in a dictionary of
        arrays for each navigation datagram.  Position information is in arrays
        ordered as time, latitude, longitude.  Attitude information is in
        arrays ordered as time, roll, pitch, heave, heading.
        Only an array for the first attitude sensor and first positioning
        sensor is pulled.
        """
        self.navarray = {}
        if not self.mapped:
            self.mapfile()
        if '80' in self.map.packdir:
            print('creating position array')
            numpos = len(self.map.packdir['80'])
            self.navarray['80'] = np.zeros((numpos, 3))
            for i in range(numpos):
                self.getrecord(80, i)
                if (self.packet.subpack.header['System'] & 3) == 1:
                    self.navarray['80'][i, 0] = self.packet.time
                    self.navarray['80'][i, 2] = self.packet.subpack.header[2]
                    self.navarray['80'][i, 1] = self.packet.subpack.header[3]
        if '65' in self.map.packdir:
            print('creating attitude array (65)')
            time = []
            roll = []
            pitch = []
            heave = []
            heading = []
            numatt = len(self.map.packdir['65'])
            for m in range(numatt):
                p65 = self.getrecord(65, m)
                if (p65.sensor_descriptor & 16) == 0:
                    time += list(p65.data['Time'])
                    roll += list(p65.data['Roll'])
                    pitch += list(p65.data['Pitch'])
                    heave += list(p65.data['Heave'])
                    heading += list(p65.data['Heading'])
            self.navarray['65'] = np.asarray(list(zip(time, roll, pitch, heave, heading)))

        if allrecords and '110' in self.map.packdir:
            print('creating attitude array (110)')
            time = []
            roll = []
            pitch = []
            heave = []
            heading = []
            exttime = []
            rollrate = []
            pitchrate = []
            yawrate = []
            downvel = []
            numatt = len(self.map.packdir['110'])
            pav = self.getrecord(110, 0)
            if pav.source == 'GRP102':
                for m in range(numatt):
                    pav = self.getrecord(110, m)
                    time += list(pav.data['Time'])
                    roll += list(pav.data['Roll'])
                    pitch += list(pav.data['Pitch'])
                    heave += list(pav.data['Heave'])
                    heading += list(pav.data['Heading'])
                    exttime += list(pav.source_data['Time1'].astype(np.float64) + pav._weektime)
                    rollrate += list(pav.source_data['RollRate'])
                    pitchrate += list(pav.source_data['PitchRate'])
                    yawrate += list(pav.source_data['YawRate'])
                    downvel += list(pav.source_data['DownVelocity'])
            elif pav.source == 'binary23':
                for m in range(numatt):
                    pav = self.getrecord(110, m)
                    time += list(pav.data['Time'])
                    roll += list(pav.data['Roll'])
                    pitch += list(pav.data['Pitch'])
                    heave += list(pav.data['Heave'])
                    heading += list(pav.data['Heading'])
                    exttime += list(pav.source_data['Seconds'] + pav.source_data['FracSeconds'])
                    rollrate += list(pav.source_data['RollRate'])
                    pitchrate += list(pav.source_data['PitchRate'])
                    yawrate += list(pav.source_data['YawRate'])
                    downvel += list(pav.source_data['DownVelocity'])
            elif pav.source == 'binary11':
                for m in range(numatt):
                    pav = self.getrecord(110, m)
                    if pav.source == 'binary11':
                        time += list(pav.data['Time'])
                        roll += list(pav.data['Roll'])
                        pitch += list(pav.data['Pitch'])
                        heave += list(pav.data['Heave'])
                        heading += list(pav.data['Heading'])
                        exttime += list(pav.source_data['Seconds'] + pav.source_data['FracSeconds'])
                        rollrate += list(pav.source_data['RollRate'])
                        pitchrate += list(pav.source_data['PitchRate'])
                        yawrate += list(pav.source_data['YawRate'])
                        downvel += list(pav.source_data['DownVelocity'])
            self.navarray['110'] = np.asarray(
                list(zip(time, roll, pitch, heave, heading, exttime, rollrate, pitchrate, yawrate, downvel)))
        if '104' in self.map.packdir:
            print('creating altitude (depth) array')
            num = len(self.map.packdir['104'])
            self.navarray['104'] = np.zeros((num, 2))
            for n in range(num):
                self.getrecord(104, n)
                self.navarray['104'][n, 0] = self.packet.gettime()
                self.navarray['104'][n, 1] = self.packet.subpack.header['Height']

    def save_navarray(self):
        """
        Creats an 'npy' file with the name of the all file that contains the
        navigation array used.
        """
        if 'navarray' not in self.__dict__:
            self._build_navarray()
        try:
            navfilename = self.infilename + '.nav'
            navfile = open(navfilename, 'wb')
            pickle.dump(self.navarray, navfile)
            navfile.close()
            print("Saved navarray to " + navfilename)
        except:
            pass

    def load_navarray(self):
        """
        Loads an 'npy' file with the name of the all file that contains the
        navigation array for this file name.
        """
        try:
            navfilename = self.infilename + '.nav'
            navfile = open(navfilename, 'rb')
            self.navarray = pickle.load(navfile)
            print("Loaded navarray from " + navfilename)
            navfile.close()
        except:
            print("No navarray file found.")

    def plot_navarray(self):
        """
        Plots the parts of the navarray.
        """
        if not hasattr(self, 'navarray'):
            self._build_navarray()
        fig = plt.figure()
        ax1 = fig.add_subplot(221)
        ax2 = fig.add_subplot(222)
        ax3 = fig.add_subplot(614, sharex=ax2)
        ax4 = fig.add_subplot(615, sharex=ax2)
        ax5 = fig.add_subplot(616, sharex=ax2)
        ax1.plot(self.navarray['80'][:, 1], self.navarray['80'][:, 2])
        ax1.set_xlabel('Longitude (Degrees)')
        ax1.set_ylabel('Latitude (Degrees)')
        ax1.grid()
        ax2.plot(self.navarray['65'][:, 0], self.navarray['65'][:, 4])
        ax2.set_ylabel('Heading (Degrees)')
        ax2.set_xlabel('Time (Seconds)')
        ax2.grid()
        if '104' in self.navarray:
            ax3.plot(self.navarray['104'][:, 0], self.navarray['104'][:, 1])
        ax3.set_ylabel('Height (Meters)')
        ax3.grid()
        ax4.plot(self.navarray['65'][:, 0], self.navarray['65'][:, 1])
        ax4.plot(self.navarray['65'][:, 0], self.navarray['65'][:, 2])
        ax4.set_ylabel('Degress')
        ax4.legend(('Roll', 'Pitch'))
        ax4.grid()
        ax5.plot(self.navarray['65'][:, 0], self.navarray['65'][:, 3])
        ax5.set_ylabel('Heave (Meters)')
        ax5.set_xlabel('Time (Seconds)')
        ax5.grid()
        ax5.set_xlim((self.navarray['65'][:, 0].min(), self.navarray['65'][:, 0].max()))
        plt.draw()

    def _build_speed_array(self):
        """
        This method builds a speed array.  First it looks for speed in the
        positon (80d) datagram.  If not available the speed is calculated from
        the position in the navarray.
        """
        self.getrecord(80, 0)
        if self.packet.subpack.header['Speed'] > 655:
            if not hasattr(self, 'navarray'):
                self._build_navarray()
            numpts = len(self.navarray['80']) - 1
            self.speedarray = np.zeros((numpts, 2))
            self.utmzone = int((180. + self.navarray['80'][0, 1]) / 6) + 1
            toutm = pyproj.Proj(proj='utm', zone=self.utmzone, ellps='WGS84')
            a, b = toutm(self.navarray['80'][:, 1], self.navarray['80'][:, 2])
            da = a[:-1] - a[1:]
            db = b[:-1] - b[1:]
            dt = self.navarray['80'][:-1, 0] - self.navarray['80'][1:, 0]
            self.speedarray[:, 1] = np.abs(np.sqrt(da ** 2 + db ** 2) / dt)
            self.speedarray[:, 0] = self.navarray['80'][:-1, 0] + dt / 2
        else:
            num80 = len(self.map.packdir['80'])
            self.speedarray = np.zeros((num80, 2))
            for n in range(num80):
                self.getrecord(80, n)
                self.speedarray[n, 1] = self.packet.subpack.header['Speed']
                self.speedarray[n, 0] = self.packet.gettime()

    def getspeed(self, tstamps, time_to_average=1):
        """
        Calling the method with time stamps returns the speed at the time. An
        additional kwarg, time_to_average, defaults to 1.  This is the window on
        either side of the times provided that the speeds will be averaged over.
        If there are no times available the closest time is provided, unless
        outside of the range of the file, in which case Nan is returned for that
        value.
        """
        tta = time_to_average
        if not hasattr(self, 'speedarray'):
            self._build_speed_array()
        # make incoming tstamp shape more flexible
        tstamps = np.asarray(tstamps)
        ndim = tstamps.shape
        if len(ndim) == 0:
            tstamps = np.array([tstamps])
        elif len(ndim) == 2:
            tstamps = tstamps[0]
        numpts = len(tstamps)
        speeds = np.zeros(numpts)
        for n in range(numpts):
            idx = np.nonzero((self.speedarray[:, 0] > tstamps[n] - tta) & (self.speedarray[:, 0] < tstamps[n] + tta))[0]
            if len(idx) > 0:
                speeds[n] = self.speedarray[idx, 1].mean()
            else:
                dt = (self.speedarray[:-1, 0] - self.speedarray[1:, 0]).mean()
                if (tstamps[n] > self.speedarray[0, 0] - dt) and tstamps[n] < self.speedarray[-1, 0] + dt:
                    idx = np.abs(self.speedarray[:, 0] - tstamps[n]).argmin()
                    speeds[n] = self.speedarray[idx, 1]
                else:
                    speeds[n] = np.nan
        return speeds

    def _get_nav_stats(self):
        """
        This method is intended to augment the correlation of motion artifacts
        with bathymetry wobbles by providing the frequency content of the
        attitude data.
        """
        if not hasattr(self, 'navarray'):
            self._build_navarray()
        if not hasattr(self, 'speedarray'):
            self._build_speed_array()
        att = self.navarray['65']
        t = att[:, 0]
        dt = (t[1:] - t[:-1]).mean()
        f = np.fft.fftfreq(len(t), d=dt)
        win = np.hanning(len(t))
        filt = np.hanning(100)
        filt /= filt.sum()
        roll_fft = np.convolve(np.fft.fft(win * att[:, 1]), filt, mode='same')
        pitch_fft = np.convolve(np.fft.fft(win * att[:, 2]), filt, mode='same')
        heave_fft = np.convolve(np.fft.fft(win * att[:, 3]), filt, mode='same')
        plt.figure()
        plt.plot(f, np.log10(np.abs(roll_fft)))
        plt.plot(f, np.log10(np.abs(pitch_fft)))
        plt.plot(f, np.log10(np.abs(heave_fft)))
        plt.xlabel('Frequency (Hz)')
        plt.legend(('Roll', 'Pitch', 'Heave'))
        plt.grid()

    def _build_runtime_array(self):
        """
        This function builds an array of all the runtime parameters.
        """
        rtp = np.dtype([('Time', 'd'), ('RuntimePacket', Data82.hdr_dtype)])
        if not self.mapped:
            self.mapfile()
        num = len(self.map.packdir['82'])
        self._runtime_array = np.zeros(num, dtype=rtp)
        for n in range(num):
            self.getrecord(82, n)
            self._runtime_array[n]['Time'] = self.packet.gettime()
            self._runtime_array[n]['RuntimePacket'] = self.packet.subpack.header

    def getruntime(self, time, values=[]):
        """
        This method provides runtime information based on what was valid for
        the provided time stamp.  The time stamp is to be provided in POSIX
        time.  If the "value" kwarg is a value from the runtime parameters that
        value is returned.  Otherwise the whole runtime parameter subrecord is
        returned.  If an invalid time is provided "None" is returned.
        """
        if not hasattr(self, 'runtime_array'):
            self._build_runtime_array()
        idx = np.nonzero(self._runtime_array['Time'] < time)[0]
        if len(idx) > 0:
            idx = idx[-1]
        else:
            # the first records are often before the first runtime record
            idx = 0
        if len(values) > 0:
            if set(values) <= set(self._runtime_array['RuntimePacket'].dtype.fields):
                return self._runtime_array['RuntimePacket'][values][idx]
            else:
                return None
        else:
            return self._runtime_array[idx]['RuntimePacket']

    def _build_sscast_array(self):
        """
        This function builds an array of all the sound speed casts.
        """
        cast_dtype = np.dtype([('Time', 'd'), ('SSCast', np.object)])
        if not self.mapped:
            self.mapfile()
        num = len(self.map.packdir['85'])
        self._sscast_array = np.zeros(num, dtype=cast_dtype)
        for n in range(num):
            ping85 = self.getrecord(85, n)
            self._sscast_array[n]['Time'] = ping85.POSIXtime
            self._sscast_array[n]['SSCast'] = ping85

    def getsscast(self, time):
        """
        This method provides cast information based on what was valid for
        the provided time stamp.  The time stamp is to be provided in POSIX
        time. Two arrays are returned.  The first is the sound speed header,
        the second is the sound speed data.  Both are in the sound speed
        datapacket data type.
        """
        if not hasattr(self, '_sscast_array'):
            self._build_sscast_array()
        idx = np.nonzero(self._sscast_array['Time'] < time)[0]
        if len(idx) > 0:
            return self._sscast_array[idx[-1]]['SSCast']
        else:
            return None

    def getping(self, pingtime=0, pingnumber=0, recordlist=[], extra=True):
        """
        This method provides all the datagrams and navigational information 
        associated with a ping.  Provide a keyword argument for either the time
        of the ping (kwarg pingtime) or the ping number (kwarg pingnumber) in 
        the file.  If both a time stamp and a ping number are provided the time
        stamp is used.  The navigation, attitude, runtime parameters and sound
        speed profile valid for the ping are also provided if the 'extra' kwarg
        is set to True.
        ***This method has not been properly tested***
        """
        if not self.mapped:
            self.mapfile()
        # first get the ping number if the ping time is provided
        if pingtime != 0:
            if '88' in self.map.packdir:
                n = '88'
            elif '68' in self.map.packdir:
                n = '68'
            else:
                n = None
            if n is not None:
                b = np.isclose([self.map.packdir['88'][:, 1]], [pingtime], rtol=0, atol=5e-4)
                idx = np.nonzero(b)[0]
                if len(idx) == 0:
                    pingnumber = None
                elif len(idx) == 1:
                    pingnumber = idx[0]
                else:
                    pingnumber = None
                    print('More than one ping with matching time stamp found.')
            # get the ping number in the file
            # pingnumber = xxx
            # if time is not found pingnumber = None
        if pingnumber is not None:
            # find the records available
            filerecords = list(self.map.packdir.keys())
            # make a list of the records to get depending on the type.
            if len(recordlist) == 0:
                # if no records specifically requested, get all (useful) available
                # XYZ
                if '88' in filerecords:
                    recordlist.append('88')
                elif '68' in filerecords:
                    recordlist.append('68')
                # Seabed Imagry
                if '89' in filerecords:
                    recordlist.append('89')
                elif '83' in filerecords:
                    recordlist.append('83')
                    # Range / Angle
                if '78' in filerecords:
                    recordlist.append('78')
                elif '102' in filerecords:
                    recordlist.append('102')
                # water column
                if '107' in filerecords:
                    recordlist.append('107')
            else:
                recordlist = [str(n) for n in recordlist]  # Support records as numbers
                for n in recordlist:
                    if n not in filerecords:
                        recordlist.remove(n)
            # now that the records to get are established as in the file...
            if len(recordlist) > 0:
                subpack = {}
                for n in recordlist:
                    if n != '107':
                        subpack[n] = self.getrecord(n, pingnumber)
                    elif n == '107':
                        subpack[n] = self.getwatercolumn(pingnumber)
                temp = copy.deepcopy(self.packet)
                if extra:
                    if pingtime == 0:
                        pingtime = self.packet.gettime()
                    subpack['speed'] = self.getspeed(pingtime)
                    subpack['Navigation'] = self.getnav(pingtime)
                    subpack['Runtime'] = self.getruntime(pingtime)
                    subpack['SoundSpeed'] = self.getsscast(pingtime)
                self.packet = temp
                # these fields are meaningless at this point.
                self.packet.header['Type'] = 0
                self.packet.header['Bytes'] = 0
                del self.packet.datablock
                self.packet.subpack = subpack
                return subpack
            return None
        return None


class Datagram:
    """
    The datagram holder.  Reads the header section of the provided memory
    block and holds a list with the datagram information through the time
    stamp.  Also, the datagram type is stored in variable 'dtype'.  Flags
    are set to indicate whether the rest of the datagram has been decoded,
    and the decoded data is stored in a datagram specific object called
    'subpack'. The maketime method is called upon decoding the record, and
    a 'time' variable is created containing a POSIX time with the packet
    time. 'valid' indicates whether the sync pattern is present, 'decoded'
    indicated if the datagram has been decoded, and 'checksum' contains the
    checksum field.
    Note: While not required of these datagrams, the size of the datagram, as
    if coming from a file, is expected at the beginning of these datablocks.
    """

    hdr_dtype = np.dtype([('Bytes', 'I'), ('Start', 'B'), ('Type', 'B'),
                          ('Model', 'H'), ('Date', 'I'), ('Time', 'I')])

    def __init__(self, fileblock, byteswap=False):
        """Reads the header section, which is the first 16 bytes, of the
        given memory block."""
        self.byteswap = byteswap
        hdr_sz = Datagram.hdr_dtype.itemsize
        self.header = np.frombuffer(fileblock[:hdr_sz], dtype=Datagram.hdr_dtype)
        if byteswap:
            self.header = self.header.byteswap()
        self.header = self.header[0]
        self.decoded = False
        if self.header[1] == 2:
            self.valid = True
        else:
            self.valid = False
        self.datablock = fileblock[hdr_sz:-3]
        # Storing data skipped in datablock
        self.datablockheader = fileblock[:hdr_sz]
        self.datablockfooter = fileblock[-3:]

        etx = np.frombuffer(fileblock[-3:-2], dtype=np.uint8, count=1)[0]
        if etx != 3:
            self.valid = False
        if byteswap:
            self.checksum = np.frombuffer(fileblock[-2:], dtype=np.uint16, count=1)[0].newbyteorder()
        else:
            self.checksum = np.frombuffer(fileblock[-2:], dtype=np.uint16, count=1)[0]
        self.dtype = self.header[2]
        try:
            self.maketime()
        except ValueError:
            pass

    def rawdatablock(self):
        return self.datablockheader + self.datablock + self.datablockfooter

    def decode(self):
        """
        Directs to the correct decoder.
        """
        if self.dtype == 48:
            self.subpack = Data48(self.datablock, self.byteswap)
        elif self.dtype == 49:
            self.subpack = Data49(self.datablock, self.byteswap)
        elif self.dtype == 51:
            self.subpack = Data51(self.datablock, self.header['Model'], self.byteswap)
        elif self.dtype == 65:
            self.subpack = Data65(self.datablock, self.time, self.byteswap)
        elif self.dtype == 66:
            self.subpack = Data66(self.datablock, self.header['Model'], self.byteswap)
        elif self.dtype == 67:
            self.subpack = Data67(self.datablock, self.byteswap)
        elif self.dtype == 68:
            self.subpack = Data68(self.datablock, self.byteswap)
        elif self.dtype == 71:
            self.subpack = Data71(self.datablock, self.time, self.byteswap)
        elif self.dtype == 73:
            self.subpack = Data73(self.datablock, self.header['Model'], self.time, self.byteswap)
        elif self.dtype == 78:
            self.subpack = Data78(self.datablock, self.time, self.byteswap)
        elif self.dtype == 79:
            self.subpack = Data79(self.datablock, self.time, self.byteswap)
        elif self.dtype == 80:
            self.subpack = Data80(self.datablock, self.time, self.byteswap)
        elif self.dtype == 82:
            self.subpack = Data82(self.datablock, self.time, self.byteswap)
        elif self.dtype == 83:
            self.subpack = Data83(self.datablock, self.byteswap)
        elif self.dtype == 85:
            self.subpack = Data85(self.datablock, self.byteswap)
        elif self.dtype == 88:
            self.subpack = Data88(self.datablock, self.byteswap)
        elif self.dtype == 89:
            self.subpack = Data89(self.datablock, self.time, self.byteswap)
        elif self.dtype == 102:
            self.subpack = Data102(self.datablock, self.byteswap)
        elif self.dtype == 104:
            self.subpack = Data104(self.datablock, self.byteswap)
        elif self.dtype == 105:
            # same definition for this data type
            self.subpack = Data73(self.datablock, self.header['Model'], self.time, self.byteswap)
        elif self.dtype == 107:
            self.subpack = Data107(self.datablock, self.byteswap)
        elif self.dtype == 109:
            self.subpack = Data109(self.datablock, self.byteswap)
        elif self.dtype == 110:
            self.subpack = Data110(self.datablock, self.time, self.byteswap)
        else:
            raise NotImplementedError("Data record " + str(self.dtype) + " decoding is not yet supported.")
        self.decoded = True

    def maketime(self):
        """
        Makes the time stamp of the current packet as a POSIX time stamp.
        UTC is assumed.
        """
        date = str(self.header[-2])
        year = int(date[:4])
        month = int(date[4:6])
        day = int(date[6:])
        numdays = dtm.date(year, month, day).toordinal() - dtm.date(1970, 1, 1).toordinal()
        dayseconds = self.header[-1] * 0.001
        self.time = numdays * 24 * 60 * 60 + dayseconds

    def gettime(self):
        """
        Calls the method "maketime" if needed and returns the POSIX time stamp.
        """
        if 'time' not in self.__dict__:
            self.maketime()
        return self.time

    def display(self):
        """
        Displays contents of the header to the command window.
        """
        for n, name in enumerate(self.header.dtype.names):
            print(name + ' : ' + str(self.header[n]))


class BaseMeta(type):
    """metaclass to read the "hdr_dtype" attribute and convert it into usable attribute names"""

    def __new__(cls, name, bases, classdict):
        if 'hdr_dtype' in classdict:
            # map the dtype names to something python can use as a variable name
            classdict['_data_keys'] = {}
            for field in classdict['hdr_dtype'].names:
                nfield = field.replace(" ", "_").replace("/", "_").replace("(", "").replace("}", "").replace("#", "Num")
                if nfield in classdict['_data_keys']:
                    # print "Duplicate field found -- %s in class %s" % (nfield, name)
                    i = 0
                    orig_field = nfield
                    while nfield in classdict['_data_keys']:
                        i += 1
                        nfield = "%s%02d" % (orig_field, i)
                classdict['_data_keys'][nfield] = field
            # compute the raw size of the data for reading/writing to disk
            if "raw_dtype" in classdict:
                classdict['hdr_sz'] = classdict['raw_dtype'].itemsize
            else:
                classdict['hdr_sz'] = classdict['hdr_dtype'].itemsize

        return type.__new__(cls, name, bases, classdict)


class BaseData(object, metaclass=BaseMeta):
    """
    Note: By deriving classes from "BaseData" the reading of data via numpy into
    an attribute named "header" is automatically done.  It is a numpy record array.
    The names are in the hdr_type attribute from this class but also can be used
    directly by their names also.  Further the data will know how to print itself in
    string form and make a rudimentary plot (if it's based on BasePlottableData).

    Units conversions can also be specified and will be applied in the BaseData.

    To get this functionality follow these steps
    (Data49 class is a pretty clean example, Data65 is more complex)

    1) supply a "hdr_dtype" attribute that is a numpy dtype with record names like examples below

    2) optionally supply a "conversions" dictionary attribute with the names of the numpy fields
       and a multiplicative constant = {'TiltTx': 0.01, 'CenterFrequency': 10}

    3) IF the raw data is not in types that you want then supply a "raw_data" decription also,
       notice in the sample Data002 we are reading PingRate here as a short int "H"
       but the hdr_dtype will cause it to be converted to a float "f"

    4) IF you need to do advanced processing on some data (change units etc),
       override the __init__ and get_datablock, again shown in Data003

    For example:
    # this makes the simplest case, data format is already what we want
    class Data001(BaseData):   
         hdr_dtype = np.dtype([('StatusDatagramCount','H'),('SystemSerialNum','H'), ('PingRate',"f")])

    # This example will convert the PingRate from short int "H" into float "f" auto-magically
    class Data002(BaseData):
         hdr_dtype = np.dtype([('StatusDatagramCount','H'),('SystemSerialNum','H'), ('PingRate',"f")])
         raw_dtype = np.dtype([('StatusDatagramCount','H'),('SystemSerialNum','H'), ('PingRate',"H")])

    # This example will convert the PingRate from short int "H" into float "f" auto-magically
    # but also convert the units from hundreths as the integer to a float.
    # -- remember that super(Data003, self) is calling the BaseData class functions:
    class Data003(BaseData):
         hdr_dtype = np.dtype([('StatusDatagramCount','H'),('SystemSerialNum','H'), ('PingRate',"f")])
         raw_dtype = np.dtype([('StatusDatagramCount','H'),('SystemSerialNum','H'), ('PingRate',"H")])
         conversions = {'PingRate': 0.01} #ping rate was in integer hundreths
         def __init__(self, datablock, byteswap=False):
            super(Data003, self).__init__(datablock, byteswap=byteswap)
            # the conversions declared above has the effect of -- self.header['PingRate'] *= 0.01
            # you can still modify values here if need be -- addition or linear
            self.header['SystemSerialNum'] += 5
         def get_datablock(self, data=None):
            # Must convert the units back first then create the buffer
            tmp_header = self.header.copy()  # don't modify our in-memory data so make a copy first
            self.header['SystemSerialNum'] -= 5  # convert the serial number back that was manually implemented
            # ping rate will be automatically converted in the BaseData implementation
            return super(Data003, self).get_datablock(tmp_header)


    # e.g. if you create an instance of one of these three data classes like:
    mydata = DataXXX(datablock)

    # then the following would work the same:
    mydata.header['PingRate']
    mydata.header[2]
    mydata.PingRate

    # You get basic string output like these
    str(mydata)
    mydata.get_display_string()
    print(mydata)
    mydata.plot() # works if based on BasePlottableData (arrays of data -- not just single headers)
    mydata.display()  # this would also make a matplotlib plot if derived from BasePlottableData

    # Data can be exported using the get_datablock function too.
    new_datablock = mydata.get_datablock()
    """
    conversions = {}

    def __init__(self, datablock, byteswap=False, read_limit=1):
        """This gets the format for each block type and then reads the block
        read_limit is used to determine how much data is read, like a slice index.
          # By default it will read one record and read it as a single instance of the datatype (everything else will be like slices and be an array)
          [0] -- just the first record and as a single instance (easier indexing)
          # a positive integer specifies how many records it will try to read.
          [:2] -- first two records
          Zero or None will read all records.
          [:] -- read all records available
          A negative integer will read that many less than the number of records available.
          [:-1] -- reads all but the last record
        """
        try:
            raw_dtype = self.raw_dtype
        except AttributeError:
            raw_dtype = self.hdr_dtype
        if read_limit is None:
            read_limit = 0
        if read_limit > 0:
            num_packets = read_limit
        else:
            num_packets = int(len(datablock) / self.hdr_sz) - read_limit
        read_sz = self.hdr_sz * num_packets

        tmp_header = np.frombuffer(datablock[:read_sz], dtype=raw_dtype)

        if read_limit == 1:  # converts from an array to single instance of the data (special case)
            tmp_header = tmp_header[0]
        self.header = tmp_header.astype(self.hdr_dtype)
        if self.conversions:
            for k, v in list(self.conversions.items()):
                self.header[k] *= v

    def get_datablock(self, data=None):
        """data is either the data to convert into a buffer string or None in which case the default data is used.
        You would pass in a different data set from a derived class if there is units translation or something that
        must be done first
        """
        if data is None:
            data = self.header
        if self.conversions:
            data = data.copy()
            for k, v in list(self.conversions.items()):
                data[k] /= v
        try:
            raw_dtype = self.raw_dtype
        except AttributeError:
            raw_dtype = self.hdr_dtype
        tmp = data.astype(raw_dtype)
        datablock = tmp.tobytes()  # I believe this is what this should look like in Python3
        return datablock

    def get_display_string(self):
        """ Displays contents of the header to the command window. """
        result = ""
        for n, name in enumerate(self.header.dtype.names):
            result += name + ' : ' + str(self.header[name]) + "\n"
        return result

    def display(self):
        print(self.__repr__())

    def __repr__(self):
        return self.get_display_string()

    def __dir__(self):
        """Custom return of attributes since we have a custom getattr function that adds data members"""
        s = list(self.__dict__.keys())
        s.extend(list(self._data_keys.keys()))
        s.extend(dir(self.__class__))
        ns = sorted([v for v in s if v[0] != "_"])
        return ns

    def __getattr__(self, key):  # Get the value from the underlying subfield (perform any conversion necessary)
        try:
            ky2 = self._data_keys[key]  # try to access the subfield
            return self.header[ky2]
        except:
            raise AttributeError(key + " not in " + str(self.__class__))

    def __setattr__(self, key, value):  # Get the value from the underlying subfield (perform any conversion necessary)
        try:
            ky2 = self._data_keys[key]  # try to access the subfield
        except:
            super(BaseData, self).__setattr__(key, value)
        else:
            self.header[ky2] = value


class BasePlottableData(BaseData):
    def display(self):
        super(BasePlottableData, self).display()
        self.plot()

    def plot(self):
        fig, ax = plt.subplots(len(self.header.dtype.names), 1)
        for n, a in enumerate(ax):
            name = self.header.dtype.names[n]
            a.plot(self.header[name])
            a.set_title(name)
            a.set_xlim((0, len(self.header[name])))
            # a.set_xlabel('index')


class Data48(BaseData):
    """
    PU information and status 0x30 / '0' / 48.
    """
    hdr_dtype = np.dtype([('ByteOrderFlag', 'H'), ('System Serial#', 'H'),
                          ('UDPPort1', 'H'), ('UDPPort2', 'H'), ('UDPPort3', 'H'), ('UDPPort4', 'H'),
                          ('SystemDescriptor', 'I'), ('PUSoftwareVersion', 'S16'),
                          ('BSPSoftwareVersion', 'S16'), ('SonarHead/TransceiverSoftware1', 'S16'),
                          ('SonarHead/TransceiverSoftware2', 'S16'), ('HostIPAddress', 'I'),
                          ('TXOpeningAngle', 'B'), ('RXOpeningAngle', 'B'), ('Spare1', 'I'),
                          ('Spare2', 'H'), ('Spare3', 'B')])


class Data49(BaseData):
    """
    PU Status datagram 0x31 / '1' / 49.  All values are converted to degrees,
    meters, and meters per second.
    """
    hdr_dtype = np.dtype([('StatusDatagramCount', 'H'), ('SystemSerialNum', 'H'),
                          ('PingRate', 'f'), ('PingCounter', 'H'), ('SwathDistance', 'I'),
                          ('SensorInputStatusUDP2', 'I'), ('SensorInputStatusSerial1', 'I'),
                          ('SensorInputStatusSerial2', 'I'), ('SensorInputStatusSerial3', 'I'),
                          ('SensorInputStatusSerial4', 'I'), ('PPSstatus', 'b'),
                          ('PositionStatus', 'b'), ('AttitudeStatus', 'b'), ('ClockStatus', 'b'),
                          ('HeadingStatus', 'b'), ('PUstatus', 'B'), ('LastHeading', "f"),
                          ('LastRoll', "f"), ('LastPitch', "f"), ('LastSonarHeave', "f"),
                          ('TransducerSoundSpeed', "f"), ('LastDepth', "f"), ('ShipVelocity', "f"),
                          ('AttitudeVelocityStatus', 'B'), ('MammalProtectionRamp', 'B'),
                          ('BackscatterOblique', 'b'), ('BackscatterNormal', 'b'), ('FixedGain', 'b'),
                          ('DepthNormalIncidence', 'B'), ('RangeNormalIncidence', 'H'),
                          ('PortCoverage', 'B'), ('StarboardCoverage', 'B'),
                          ('TransducerSoundSpeedFromProfile', "f"), ('YawStabAngle', "f"),
                          ('PortCoverageORAbeamVelocity', 'h'),
                          ('StarboardCoverageORDownVelocity', 'h'), ('EM2040CPUTemp', 'b')])

    raw_dtype = np.dtype([('StatusDatagramCount', 'H'), ('SystemSerialNum', 'H'),
                          ('PingRate', "H"), ('PingCounter', 'H'), ('SwathDistance', 'I'),
                          ('SensorInputStatusUDP2', 'I'), ('SensorInputStatusSerial1', 'I'),
                          ('SensorInputStatusSerial2', 'I'), ('SensorInputStatusSerial3', 'I'),
                          ('SensorInputStatusSerial4', 'I'), ('PPSstatus', 'b'),
                          ('PositionStatus', 'b'), ('AttitudeStatus', 'b'), ('ClockStatus', 'b'),
                          ('HeadingStatus', 'b'), ('PUstatus', 'B'), ('LastHeading', "H"),
                          ('LastRoll', "h"), ('LastPitch', "h"), ('LastSonarHeave', "h"),
                          ('TransducerSoundSpeed', "H"), ('LastDepth', "I"), ('ShipVelocity', "h"),
                          ('AttitudeVelocityStatus', 'B'), ('MammalProtectionRamp', 'B'),
                          ('BackscatterOblique', 'b'), ('BackscatterNormal', 'b'), ('FixedGain', 'b'),
                          ('DepthNormalIncidence', 'B'), ('RangeNormalIncidence', 'H'),
                          ('PortCoverage', 'B'), ('StarboardCoverage', 'B'),
                          ('TransducerSoundSpeedFromProfile', "H"), ('YawStabAngle', "h"),
                          ('PortCoverageORAbeamVelocity', 'h'),
                          ('StarboardCoverageORDownVelocity', 'h'), ('EM2040CPUTemp', 'b')])

    conversions = {'PingRate': 0.01, 'LastHeading': 0.01, 'LastRoll': 0.01, 'LastPitch': 0.01,
                   'LastSonarHeave': 0.01, 'TransducerSoundSpeed': 0.1, 'LastDepth': 0.01,
                   'ShipVelocity': 0.01, 'TransducerSoundSpeedFromProfile': 0.01, 'YawStabAngle': 0.01,
                   }

    def __init__(self, datablock, byteswap=False):
        """Catches the binary datablock and decodes the first section and calls
        the decoder for the rest of the record."""
        super(Data49, self).__init__(datablock, byteswap=byteswap)


class Data51(BaseData):
    """
    ExtraParameters datagram.
    """

    hdr_dtype = np.dtype([('Counter', 'H'), ('serial#', 'H'), ('ContentIdentifier', 'H')])

    def __init__(self, datablock, model, byteswap=False):
        """Catches the binary datablock and decodes the first section and calls
        the decoder for the rest of the record."""
        super(Data51, self).__init__(datablock, byteswap=byteswap)
        self.model = model
        content_type = self.header[-1]
        if content_type == 1:
            self.data = datablock[self.hdr_sz:self.hdr_sz + 100]
        elif content_type == 2:
            pass
        elif content_type == 3:
            pass
        elif content_type == 4:
            pass
        elif content_type == 5:
            pass
        elif content_type == 6:
            data_sz = np.frombuffer(datablock[self.hdr_sz: self.hdr_sz + 2], dtype='H')[0]
            self.rawdata = datablock[self.hdr_sz + 2: self.hdr_sz + 2 + data_sz]
            if model == 710:
                self._parse_710_bscorr()
            elif model == 122:
                self._parse_122_bscorr()

    def _parse_710_bscorr(self):
        """
        Parse the BSCorr file.
        """
        c = self.rawdata.split('#')
        t = len(c)
        n = 0
        self.names = []
        self.swathnum = []
        self.modes = []
        self.powers = []
        self.data = []
        while n < t:
            if len(c[n]) > 0:
                header = c[n].split('\n')
                self.names.append(header[0])
                info = [int(x) for x in header[1].split('\t')]
                self.modes.append(info[0])
                self.swathnum.append(info[1])
                numsectors = info[2]
                sector = []
                secpower = []
                for m in range(numsectors):
                    n += 1
                    sectordata = c[n].split('\n')
                    sectorname = sectordata[0]
                    secpower.append(float(sectordata[1]))
                    numpts = int(sectordata[2])
                    angle = []
                    offset = []
                    for k in range(numpts):
                        a, d = sectordata[k + 3].split('\t')
                        angle.append(float(a))
                        offset.append(float(d))
                    sector.append(np.array(list(zip(angle, offset))))
                self.data.append(np.array(sector))
                self.powers.append(np.array(secpower))
            n += 1

    def _parse_122_bscorr(self):
        """
        Parse the BSCorr file.
        """
        c = self.rawdata.split('\n')
        t = len(c) - 1  # no need to look at hex flag at end
        n = 0
        header = True
        section_idx = []
        self.names = []
        self.swathnum = []
        self.modes = []
        self.powers = []
        self.data = []
        # find all the sections to be parsed
        while n < t:
            if header == False:
                if (c[n][0] == '#') & (c[n + 1][0] == '#'):
                    section_idx.append(n)
            else:
                if c[n] == '# source level    lobe angle    lobe width':
                    header = False
            n += 1
        section_idx.append(t)
        for n in range(len(section_idx) - 1):
            m = section_idx[n]
            end = section_idx[n + 1]
            name_prefix = c[m][2:]
            if name_prefix == 'Shallow':
                mode = 2
            elif name_prefix == 'Medium':
                mode = 3
            elif name_prefix == 'Deep':
                mode = 4
            elif name_prefix == 'Very Deep':
                mode = 5
            else:
                mode = -1
                print('mode type not recognized: ' + name_prefix)
            m += 1
            while m < end:
                if c[m][0] == '#':
                    k = 1
                    data = []
                    swath_type = c[m][2:]
                    if swath_type[:12] == 'Single swath':
                        swath_num = 0
                    elif swath_type == 'Dual swath 1':
                        swath_num = 1
                    elif swath_type == 'Dual swath 2':
                        swath_num = 2
                    else:
                        swath_num = -1
                        # print 'swath type not used: ' + swath_type
                    self.names.append(name_prefix + ' - ' + swath_type)
                    self.swathnum.append(swath_num)
                    self.modes.append(mode)
                    while c[m + k][0] != '#' and m + k + 1 < end:
                        info = [int(x) for x in c[m + k].split()]
                        data.append(info)
                        k += 1
                    self.data.append(data)
                m += 1

    def plot_BSCorr(self, mode_number):
        """
        This is a hack to quickly display BSCorr files.  The mode number to
        to provide to this method is an integer, starting at 0, that
        conrrisponds to the mode in the BSCorr file.  Print the this object's
        'names' variable to see the order they are stored in.
        """
        if self.header[-1] == 6:
            data = self.data[mode_number]
            numswaths = len(data)
            fig, ax = plt.subplots()
            for n in range(numswaths):
                ax.plot(data[n][:, 0], data[n][:, 1], 'o:')
            r = ax.get_xlim()
            ax.set_xlim((max(r), min(r)))
            ax.set_xlabel('Beam Angle (deg, negative is to STBD)')
            ax.set_ylabel('BS Adjustment (dB)')
            ax.set_title('BSCorr: ' + self.names[mode_number])
            ax.grid()

    def display(self):
        """
        Displays contents of the header to the command window.
        """
        super(Data51, self).display()
        if 'data' in self.__dict__:
            print(self.data)


class Data65_att(BasePlottableData):
    hdr_dtype = np.dtype([('Time', 'd'), ('Status', 'H'), ('Roll', 'f'), ('Pitch', 'f'),
                          ('Heave', 'f'), ('Heading', 'f')])
    raw_dtype = np.dtype([('Time', 'H'), ('Status', 'H'), ('Roll', 'h'), ('Pitch', 'h'),
                          ('Heave', 'h'), ('Heading', 'H')])
    conversions = {'Roll': 0.01, 'Pitch': 0.01, 'Heave': 0.01, 'Heading': 0.01}

    def __init__(self, datablock, POSIXtime, byteswap=False, read_limit=None):
        """Catches the binary datablock and decodes the first section and calls
        the decoder for the rest of the record."""
        super(Data65_att, self).__init__(datablock, byteswap=byteswap,
                                         read_limit=read_limit)  # read as many records as passed in
        self.time = POSIXtime
        self.header['Time'] = self.header['Time'] * 0.001 + self.time

    def get_datablock(self, data=None):
        tmp_header = self.header.copy()
        tmp_header['Time'] = (self.header['Time'] - self.time) * 1000
        return super(Data65_att, self).get_datablock(tmp_header)


class Data65(BaseData):
    """
    Attitude datagram 0x41/'A'/65. Data can be found in the array 'data' and
    is stored as time (POSIX), roll(deg), pitch(deg), heave(m),
    heading(deg).  sensor_descriptor does not appear to parse correctly...
    Perhaps it is not included in the header size so it is not sent to this
    object in the datablock?
    """

    hdr_dtype = np.dtype([('Counter', 'H'), ('serial#', 'H'), ('NumEntries', 'H')])

    def __init__(self, datablock, POSIXtime, byteswap=False):
        """Catches the binary datablock and decodes the first section and calls
        the decoder for the rest of the record."""
        super(Data65, self).__init__(datablock, byteswap=byteswap)
        self.time = POSIXtime
        self.sensor_descriptor = np.frombuffer(datablock[-1:], dtype=np.uint8)[0]
        self.att = Data65_att(datablock[self.hdr_sz:-1], POSIXtime, byteswap=byteswap)
        self.data = self.att.header

    def get_display_string(self):
        value = super(Data65, self).get_display_string()
        value += 'Sensor Descriptor : ' + np.binary_repr(self.sensor_descriptor, 8) + "\n"
        # value += self.att.get_display_string()
        return value

    def display(self):
        super(Data65, self).display()
        self.att.display()

    def get_datablock(self, data=None):
        part1 = super(Data65, self).get_datablock()
        part2 = self.att.get_datablock()
        part3 = str(np.frombuffer(self.sensor_descriptor, dtype=np.uint8))
        return part1 + part2 + part3


class Data66(BaseData):
    """
    PU BIST results output datagram 0x42/'B'/66.  The raw string text is parsed
    and provided in the array 'data' and 'metadata'.  The raw data 
    string is also available in the 'raw_data' class variable.
    """
    hdr_dtype = np.dtype([('Counter', 'H'), ('Serial#', 'H'), ('Test#', 'H'),
                          ('TestStatus', 'h')])

    def __init__(self, datablock, model, byteswap=False):
        """
        Catches the binary datablock and decodes the record.
        """
        super(Data66, self).__init__(datablock, byteswap=byteswap)
        self.raw_data = datablock[self.hdr_sz:]
        self._model = model
        if self.header['Test#'] == 9 and self._model == 2040:
            self.testtype = 'ChannelNoise'
        elif self.header['Test#'] == 10 and self._model == 2040:
            self.testtype = 'NoiseSpectrum'
        elif self.header['Test#'] == 8 and self._model == 710:
            self.testtype = 'ChannelNoise'
        elif self.header['Test#'] == 9 and self._model == 710:
            self.testtype = 'NoiseSpectrum'
        else:
            self.testtype = 'Unknown'

    def parse(self):
        """
        Parses the text section. May change with sonar type?
        """
        if self.testtype == 'ChannelNoise':
            if self._model == 2040:
                self._2040_parse_noisetest()
            elif self._model == 710:
                self._710_parse_noisetest()
        elif self.testtype == 'NoiseSpectrum':
            if self._model == 2040:
                self._2040_parse_noisespectrum()
            elif self._model == 710:
                self._710_parse_noisespectrum()
        else:
            print('\n')
            print(self.raw_data)

    def _2040_parse_noisetest(self):
        """
        Should work for the 2040 as of SIS 4.15.
        """
        lines = self.raw_data.split('\n')
        nmax = len(lines)
        n = 0
        data = []
        # spin through all lines in the text
        while n < nmax:
            line = lines[n].split()
            if len(line) > 0:
                if line[0] == 'Channel':
                    self.label = line[1:]
                    numfreq = len(self.label)
                    freqcounter = list(range(numfreq))
                    for m in freqcounter:
                        data.append([])
                    n += 1
                    while n < nmax:
                        line = lines[n].split()
                        if len(line) > 0:
                            channel = line[0]
                            for m in freqcounter:
                                data[m].append(float(line[m + 1]))
                            n += 1
                        else:
                            break
                    n = nmax
            n += 1
        self.data = np.asarray(data).T

    def _710_parse_noisetest(self):
        """
        Should work for the EM710 as of SIS 4.15.
        """
        lines = self.raw_data.split('\n')
        nmax = len(lines)
        n = 0
        data = []
        go = False
        # spin through all lines in the text
        while n < nmax:
            line = lines[n].split()
            if len(line) > 0:
                if line[0] == 'Board':
                    go = True
                elif line[0] == 'Maximum':
                    go = False
                elif go:
                    temp = [float(x) for x in line[1:-1]]
                    data.append(temp)
                else:
                    pass
            n += 1
        self.data = np.asarray(data).T

    def _2040_parse_noisespectrum(self):
        """
        For the EM2040 as of SIS 4.15.
        """
        lines = self.raw_data.split('\n')
        nmax = len(lines)
        n = 0
        data = []
        freq = []
        go = False
        # spin through all lines in the text
        while n < nmax:
            line = lines[n].split()
            if len(line) > 0:
                if line[0][:4] == '----':
                    go = True
                elif line[0] == 'Summary...:':
                    go = False
                elif go:
                    temp = [float(x) for x in line[2::2]]
                    data.append(temp)
                    freq.append(float(line[0]))
                else:
                    pass
            n += 1
        self.data = np.asarray(data)
        self.freq = np.asarray(freq)

    def _710_parse_noisespectrum(self):
        """
        For the EM710 as of SIS 4.15.
        """
        lines = self.raw_data.split('\n')
        nmax = len(lines)
        n = 0
        data = []
        freq = []
        go = False
        # spin through all lines in the text
        while n < nmax:
            line = lines[n].split()
            if len(line) > 0:
                if line[0] == 'Board':
                    go = True
                elif line[0] == 'Maximum':
                    go = False
                elif go:
                    temp = [float(x) for x in line[2:-1]]
                    data.append(temp)
                    freq.append(float(line[0]))
                else:
                    pass
            n += 1
        self.data = np.asarray(data)
        self.freq = np.asarray(freq)
        idx = self.freq.argsort()
        self.freq = self.freq[idx]
        self.data = self.data[idx]

    def plot(self):
        """
        Plots the results of the BIST test if applicable.
        """
        self.parse()
        if self.testtype == 'ChannelNoise':
            if self._model == 2040:
                fig, ax = plt.subplots(len(self.label), 1)
                for n, a in enumerate(ax):
                    a.plot(self.data[:, n])
                    a.set_title(str(self.label[n]))
                    a.set_ylabel('dB')
                    a.set_xlim((0, self.data.shape[0]))
                a.set_xlabel('Channel Number')
                fig.suptitle(('Noise'))
            elif self._model == 710:
                plt.figure()
                plt.plot(self.data.flatten())
                plt.xlabel('Channel Number')
                plt.ylabel('Noise Level (dB)')
                plt.title('EM710 Noise Test')
                plt.grid()
                plt.xlim((0, len(self.data.flatten())))
            else:
                print('Plotting of ' + self.testtype + ' is not supported for the EM' + str(self._model))
        elif self.testtype == 'NoiseSpectrum':
            if self._model == 710 or self._model == 2040:
                l, w = self.data.shape
                legend = []
                for n in range(w):
                    legend.append('Board ' + str(n))
                plt.figure()
                plt.plot(self.freq, self.data)
                plt.xlabel('Frequency (kHz)')
                plt.ylabel('Noise Level (dB)')
                plt.title('EM' + str(self._model) + ' Noise Spectrum')
                plt.legend(legend)
                plt.grid()
            else:
                print('Plotting of ' + self.testtype + ' is not supported for the EM' + str(self._model))
        else:
            print("Plotting not supported")

    def display(self):
        """
        Displays contents of the header to the command window.
        """
        super(Data66, self).display()
        print(self.raw_data)


class Data67(BaseData):
    """
    Clock datagram 043h / 67d / 'C'. Date is YYYYMMDD. Time is in miliseconds
    since midnight.
    """
    hdr_dtype = np.dtype([('ClockCounter', 'H'), ('SystemSerial#', 'H'),
                          ('Date', 'I'), ('Time', 'I'), ('1PPS', 'B')])

    def __init__(self, datablock, byteswap=False):
        """
        Catches the binary datablock and decodes the first section and calls
        the decoder for the rest of the record.
        """
        super(Data67, self).__init__(datablock, byteswap=byteswap)
        if len(datablock) > self.hdr_sz:
            print(len(datablock), self.hdr_sz)


class Data68_xyz(BasePlottableData):
    hdr_dtype = np.dtype([('Depth', "f"), ('AcrossTrack', "f"), ('AlongTrack', "f"),
                          ('BeamDepressionAngle', "f"), ('BeamAzimuthAngle', "f"),
                          ('OneWayRange', "f"), ('QualityFactor', 'B'),
                          ('DetectionWindowLength', "f"), ('Reflectivity', "f"), ('BeamNumber', 'B')])
    raw_dtype = np.dtype([('Depth', "h"), ('AcrossTrack', "h"), ('AlongTrack', "h"),
                          ('BeamDepressionAngle', "h"), ('BeamAzimuthAngle', "H"),
                          ('OneWayRange', "H"), ('QualityFactor', 'B'),
                          ('DetectionWindowLength', "B"), ('Reflectivity', "b"), ('BeamNumber', 'B')])
    conversions = {'BeamDepressionAngle': 0.01, 'BeamAzimuthAngle': 0.01, 'Reflectivity': 0.5}

    def __init__(self, datablock, data68_info, byteswap=False, read_limit=None):
        """Catches the binary datablock and decodes the first section and calls
        the decoder for the rest of the record."""
        super(Data68_xyz, self).__init__(datablock, byteswap=byteswap,
                                         read_limit=read_limit)  # read as many records as passed in
        self._zres = data68_info['Zresolution']
        self.header['Depth'] *= self.zres
        self._xyres = data68_info['XYresolution']
        self.header['AcrossTrack'] *= self._xyres
        self.header['AlongTrack'] *= self._xyres
        self._samplerate = data68_info['SampleRate']
        self.header['OneWayRange'] /= self._samplerate
        # self.header['DetectionWindowLength'] *= 4    # not sure what this is for or what it means

    def get_datablock(self, data=None):
        tmp_header = self.header.copy()
        tmp_header['Depth'] /= self.zres
        tmp_header['AcrossTrack'] /= self._xyres
        tmp_header['AlongTrack'] /= self._xyres
        tmp_header['OneWayRange'] *= self._samplerate
        # tmp_header['DetectionWindowLength'] /= 4    # not sure what this is for or what it means
        return super(Data68_xyz, self).get_datablock(tmp_header)


class Data68(BaseData):
    """
    XYZ datagram 044h / 68d / 'D'. All values are converted to meters, degrees,
    or whole units.  The header sample rate may not be correct, but is 
    multiplied by 4 to make the one way travel time per beam appear correct. The
    detection window length per beam is in its raw form...
    """
    hdr_dtype = np.dtype([('PingCounter', 'H'), ('SystemSerial#', 'H'),
                          ('VesselHeading', "f"), ('SoundSpeed', "f"), ('TransducerDepth', "f"),
                          ('MaximumBeams', 'B'), ('ValidBeams', 'B'), ('Zresolution', "f"),
                          ('XYresolution', "f"), ('SampleRate', 'f')])
    raw_dtype = np.dtype([('PingCounter', 'H'), ('SystemSerial#', 'H'),
                          ('VesselHeading', "H"), ('SoundSpeed', "H"), ('TransducerDepth', "H"),
                          ('MaximumBeams', 'B'), ('ValidBeams', 'B'), ('Zresolution', "B"),
                          ('XYresolution', "B"), ('SampleRate', 'H')])
    conversions = {2: 0.01, 3: 0.1, 4: 0.01, 7: 0.01, 8: 0.01,
                   -1: 4,  # FIXME: revisit this number... it makes the range work but may not be correct
                   }

    def __init__(self, datablock, byteswap=False):
        """
        Catches the binary datablock and decodes the first section and calls
        the decoder for the rest of the record.
        """
        super(Data68, self).__init__(datablock, byteswap=byteswap)
        self.depthoffsetmultiplier = np.frombuffer(datablock[-1:], dtype='b')[0] * 65536
        self.header[4] += self.depthoffsetmultiplier
        self.xyz = Data68_xyz(datablock[self.hdr_sz:-1], self.header, byteswap=byteswap)
        self.data = self.xyz.header

    def get_datablock(self, data=None):
        tmp_header = self.header.copy()
        tmp_header.header[4] -= self.depthoffsetmultiplier
        part1 = super(Data68, self).get_datablock(tmp_header)
        part2 = self.xyz.get_datablock()
        part3 = str(np.frombuffer([self.depthoffsetmultiplier / 65536], dtype='b'))
        return part1 + part2 + part3

    def get_display_string(self):
        s = super(Data68, self).get_display_string()
        s += 'TransducerDepthOffsetMultiplier : ' + str(self.depthoffsetmultiplier) + "\n"
        # s += self.xyz.get_display_string()
        return s

    def display(self):
        super(Data68, self).display()
        self.xyz.display()


class Data71_ss(BasePlottableData):
    hdr_dtype = np.dtype([('Time', 'd'), ('SoundSpeed', 'f')])
    raw_dtype = np.dtype([('Time', 'H'), ('SoundSpeed', 'H')])
    conversions = {'SoundSpeed': 0.1}

    def __init__(self, datablock, POSIXtime, byteswap=False, read_limit=None):
        """Catches the binary datablock and decodes the first section and calls
        the decoder for the rest of the record."""
        super(Data71_ss, self).__init__(datablock, byteswap=byteswap,
                                        read_limit=read_limit)  # read as many records as passed in
        self._time = POSIXtime
        self.header['Time'] += self._time

    def get_datablock(self, data=None):
        tmp_header = self.header.copy()
        tmp_header['Time'] -= self._time
        return super(Data71_ss, self).get_datablock(tmp_header)


class Data71(BaseData):
    """
    Surface Sound Speed datagram 047h / 71d / 'G'.  Time is in POSIX time and
    sound speed is in meters per second.
    """
    hdr_dtype = np.dtype([('SoundSpeedCounter', 'H'), ('SystemSerial#', 'H'),
                          ('NumEntries', 'H')])

    def __init__(self, datablock, POSIXtime, byteswap=False):
        """
        Catches the binary datablock and decodes the first section and calls
        the decoder for the rest of the record.
        """
        super(Data71, self).__init__(datablock, byteswap=byteswap)
        self.time = POSIXtime
        self.ss = Data71_ss(datablock[self.hdr_sz:-1], POSIXtime, byteswap=byteswap)
        self.data = self.ss.header
        self.endchar = datablock[-1]

    def get_datablock(self, data=None):
        part1 = super(Data71, self).get_datablock()
        part2 = self.ss.get_datablock()
        part3 = self.endchar
        return part1 + part2 + part3


class Data73(BaseData):
    """
    Installation parameters datagram 049h (start) / 73d / 'I', 069h(stop)/ 105d
    / 'I' or 70h(remote) / 112d / 'r'.  There is a short header section and the
    remainder of the record is ascii, comma delimited.
    """
    hdr_dtype = np.dtype([('SurveyLine#', 'H'), ('Serial#', 'H'), ('Serial#2', 'H')])

    def __init__(self, datablock, modelnum, POSIXtime, byteswap=False):
        """
        Catches the binary datablock and decodes the first section and calls
        the decoder for the rest of the record.
        """
        super(Data73, self).__init__(datablock, byteswap=byteswap)
        temp = datablock[self.hdr_sz:].rstrip(b'\x00').decode('utf8').split(',')
        self.settings = {}
        self.ky_data73_translator = {'WLZ': 'waterline_vertical_location', 'SMH': 'system_main_head_serial_number',
                                     'HUN': 'hull_unit', 'HUT': 'hull_unit_offset', 'TXS': 'tx_serial_number',
                                     'T2X': 'tx_no2_serial_number', 'R1S': 'rx_no1_serial_number',
                                     'R2S': 'rx_no2_serial_number', 'STC': 'system_transducer_configuration',
                                     'S0Z': 'transducer_0_vertical_location', 'S0X': 'transducer_0_along_location',
                                     'S0Y': 'transducer_0_athwart_location', 'S0H': 'transducer_0_heading_angle',
                                     'S0R': 'transducer_0_roll_angle', 'S0P': 'transducer_0_pitch_angle',
                                     'S1Z': 'transducer_1_vertical_location', 'S1X': 'transducer_1_along_location',
                                     'S1Y': 'transducer_1_athwart_location', 'S1H': 'transducer_1_heading_angle',
                                     'S1R': 'transducer_1_roll_angle', 'S1P': 'transducer_1_pitch_angle',
                                     'S1N': 'transducer_1_number_modules', 'S2Z': 'transducer_2_vertical_location',
                                     'S2X': 'transducer_2_along_location', 'S2Y': 'transducer_2_athwart_location',
                                     'S2H': 'transducer_2_heading_angle', 'S2R': 'transducer_2_roll_angle',
                                     'S2P': 'transducer_2_pitch_angle', 'S2N': 'transducer_2_number_modules',
                                     'S3Z': 'transducer_3_vertical_location', 'S3X': 'transducer_3_along_location',
                                     'S3Y': 'transducer_3_athwart_location', 'S3H': 'transducer_3_heading_angle',
                                     'S3R': 'transducer_3_roll_angle', 'S3P': 'transducer_3_pitch_angle',
                                     'S1S': 'tx_array_size', 'S2S': 'rx_array_size', 'GO1': 'sonar_head_1_gain_offset',
                                     'GO2': 'sonar_head_2_gain_offset', 'OBO': 'outer_beam_offset',
                                     'FGD': 'high_low_freq_gain_difference', 'TSV': 'transmitter_software_version',
                                     'RSV': 'receiver_software_version', 'BSV': 'bsp_software_version',
                                     'PSV': 'processing_unit_software_version', 'DDS': 'dds_software_version',
                                     'OSV': 'operator_station_software_version', 'DSV': 'datagram_format_version',
                                     'DSX': 'pressure_sensor_along_location', 'DSY': 'pressure_sensor_athwart_location',
                                     'DSZ': 'pressure_sensor_vertical_location', 'DSD': 'pressure_sensor_time_delay',
                                     'DSO': 'pressure_sensor_offset', 'DSF': 'pressure_sensor_scale_factor',
                                     'DSH': 'pressure_sensor_heave', 'APS': 'active_position_system_number',
                                     'P1Q': 'position_1_quality_check', 'P1M': 'position_1_motion_compensation',
                                     'P1T': 'position_1_time_stamp', 'P1Z': 'position_1_vertical_location',
                                     'P1X': 'position_1_along_location', 'P1Y': 'position_1_athwart_location',
                                     'P1D': 'position_1_time_delay', 'P1G': 'position_1_geodetic_datum',
                                     'P2Q': 'position_2_quality_check', 'P2M': 'position_2_motion_compensation',
                                     'P2T': 'position_2_time_stamp', 'P2Z': 'position_2_vertical_location',
                                     'P2X': 'position_2_along_location', 'P2Y': 'position_2_athwart_location',
                                     'P2D': 'position_2_time_delay', 'P2G': 'position_2_geodetic_datum',
                                     'P3Q': 'position_3_quality_check', 'P3M': 'position_3_motion_compensation',
                                     'P3T': 'position_3_time_stamp', 'P3Z': 'position_3_vertical_location',
                                     'P3X': 'position_3_along_location', 'P3Y': 'position_3_athwart_location',
                                     'P3D': 'position_3_time_delay', 'P3G': 'position_3_geodetic_datum',
                                     'P3S': 'position_3_serial_or_ethernet',
                                     'MSZ': 'motion_sensor_1_vertical_location',
                                     'MSX': 'motion_sensor_1_along_location', 'MSY': 'motion_sensor_1_athwart_location',
                                     'MRP': 'motion_sensor_1_roll_ref_plane', 'MSD': 'motion_sensor_1_time_delay',
                                     'MSR': 'motion_sensor_1_roll_angle', 'MSP': 'motion_sensor_1_pitch_angle',
                                     'MSG': 'motion_sensor_1_heading_angle',
                                     'NSZ': 'motion_sensor_2_vertical_location',
                                     'NSX': 'motion_sensor_2_along_location', 'NSY': 'motion_sensor_2_athwart_location',
                                     'NRP': 'motion_sensor_2_roll_ref_plane', 'NSD': 'motion_sensor_2_time_delay',
                                     'NSR': 'motion_sensor_2_roll_angle', 'NSP': 'motion_sensor_2_pitch_angle',
                                     'NSG': 'motion_sensor_2_heading_angle', 'GCG': 'gyrocompass_heading_offset',
                                     'MAS': 'roll_scaling_factor', 'SHC': 'transducer_depth_sound_speed_source',
                                     'PPS': '1pps_clock_sync', 'CLS': 'clock_source', 'CLO': 'clock_offset',
                                     'VSN': 'active_attitude_velocity', 'VSU': 'attitude_velocity_sensor_1_address',
                                     'VSE': 'attitude_velocity_sensor_1_port',
                                     'VTU': 'attitude_velocity_sensor_2_address',
                                     'VTE': 'attitude_velocity_sensor_2_port',
                                     'ARO': 'active_roll_pitch_sensor', 'AHE': 'active_heave_sensor',
                                     'AHS': 'active_heading_sensor', 'VSI': 'ethernet_2_address',
                                     'VSM': 'ethernet_2_network_mask', 'MCAn': 'multicast_sensor_address',
                                     'MCUn': 'multicast_sensor_port', 'MCIn': 'multicast_sensor_identifier',
                                     'MCPn': 'multicast_position_system_number', 'SNL': 'ship_noise_level',
                                     'CPR': 'cartographic_projection', 'ROP': 'responsible_operator',
                                     'SID': 'survey_identifier', 'RFN': 'raw_file_name',
                                     'PLL': 'survey_line_identifier',
                                     'COM': 'comment'}
        self.ky_data73_val_translator = {'DSH': {'IN': 'uav_depth_sensor', 'NI': 'not_depth_sensor'},
                                         'P1M': {'0': 'active', '1': 'passive'}, 'P2M': {'0': 'active', '1': 'passive'},
                                         'P3M': {'0': 'active', '1': 'passive'},
                                         'P1T': {'0': 'system_time', '1': 'position_datagram'},
                                         'P2T': {'0': 'system_time', '1': 'position_datagram'},
                                         'P3T': {'0': 'system_time', '1': 'position_datagram'},
                                         'APS': {'0': 'position_1', '1': 'position_2', '2': 'position_3'},
                                         'SHC': {'0': 'trans_ss_used_in_profile', '1': 'trans_ss_notused_in_profile'},
                                         'CLS': {'0': 'ZDA', '1': 'active_pos', '2': 'operator_station'},
                                         'VSN': {'0': 'attvel_not_used', '1': 'attvel_sensor_1',
                                                 '2': 'attvel_sensor_2'},
                                         'PPS': {'-1': 'unknown', '0': 'not_in_use', '1': 'falling_edge',
                                                 '2': 'rising_edge'},
                                         'SNL': {'0': 'normal', '1': 'high', '2': 'very_high'},
                                         'ARO': {'2': 'com2', '3': 'com3', '8': 'udp5', '9': 'udp6'},
                                         'AHE': {'2': 'com2', '3': 'com3', '8': 'udp5', '9': 'udp6'},
                                         'AHS': {'0': 'position_3_udp2', '1': 'position_1_com1', '2': 'motion_1_com2',
                                                 '3': 'motion_2_com3', '4': 'position_3_com4', '5': 'multicast_1',
                                                 '6': 'multicast_2', '7': 'multicast_3', '8': 'attvel_1_udp5',
                                                 '9': 'attvel_2_udp6'}
                                         }
        self.ky_data73_sonar_translator = {'em122': [None, 'tx', 'rx', None], 'em302': [None, 'tx', 'rx', None],
                                           'em710': [None, 'tx', 'rx', None], 'em2040': [None, 'tx', 'rx', None],
                                           'em2040_dual_rx': [None, 'tx', 'rx_port', 'rx_stbd'],
                                           'em2040_dual_tx': ['tx_port', 'tx_stbd', 'rx_port', 'rx_stbd'],
                                           # 'em2040c': [None, 'sonar_head1', 'sonar_head2', None], not sure how to identify this yet
                                           'em3002': [None, 'sonar_head1', 'sonar_head2', None],
                                           'em2040p': [None, 'sonar_head1', None, None],
                                           'me70bo': ['transducer', None, None, None]}
        self.time = POSIXtime
        for entry in temp:
            data = entry.split('=')
            if len(data) == 2:
                ky = data[0]
                if ky in self.ky_data73_val_translator:
                    val = self.ky_data73_val_translator[ky][data[1]]
                else:
                    val = data[1]
                self.settings[self.ky_data73_translator[ky]] = val
        self.settings['sonar_model_number'] = self.return_model_num(str(modelnum))

    def return_model_num(self, modelnum):
        possibles = [sonar for sonar in list(self.ky_data73_sonar_translator) if sonar.find(modelnum) > 0]
        if len(possibles) == 0:
            print('Unable to determine sonar model from {}'.format(modelnum))
            return modelnum
        elif len(possibles) == 1:
            return 'em' + modelnum
        else:
            # get here for all the 2040 variants
            offs = ['transducer_0_along_location', 'transducer_1_along_location', 'transducer_2_along_location',
                    'transducer_3_along_location']
            srch_offsets = [(off in self.settings) for off in offs]
            for poss in possibles:
                off_test = [(lvr is not None) for lvr in self.ky_data73_sonar_translator[poss]]
                if off_test == srch_offsets:
                    return poss
            print('Unable to determine sonar model from {}'.format(modelnum))
            return modelnum

    def get_display_string(self):
        """
        Displays contents of the header to the command window.
        """
        s = super(Data73, self).get_display_string()
        keys = sorted(list(self.settings.keys()))
        for key in keys:
            s += key + ' : ' + str(self.settings[key]) + "\n"
        return s

    def get_datablock(self, data=None):
        raise Exception("This data type is not exportable yet.  Need to confirm the format of the ascii data")


class Data78_ntx(BaseData):
    hdr_dtype = np.dtype([('TiltAngle', 'f'), ('Focusing', 'f'), ('SignalLength', 'f'), ('Delay', 'f'),
                          ('Frequency', 'f'), ('AbsorptionCoef', 'f'), ('WaveformID', 'B'),
                          ('TransmitSector#', 'B'), ('Bandwidth', 'f')])
    raw_dtype = np.dtype([('TiltAngle', 'h'), ('Focusing', 'H'), ('SignalLength', 'f'), ('Delay', 'f'),
                          ('Frequency', 'f'), ('AbsorptionCoef', 'H'), ('WaveformID', 'B'),
                          ('TransmitSector#', 'B'), ('Bandwidth', 'f')])
    conversions = {'TiltAngle': 0.01,  # convert to degrees
                   'Focusing': 0.1,  # convert to meters
                   'AbsorptionCoef': 0.01,  # convert to dB/km
                   }

    def __init__(self, datablock, byteswap=False, read_limit=None):
        super(Data78_ntx, self).__init__(datablock, byteswap=byteswap,
                                         read_limit=read_limit)  # read as many records as passed in


class Data78_nrx(BaseData):
    hdr_dtype = np.dtype([('BeamPointingAngle', 'f'), ('TransmitSectorID', 'B'), ('DetectionInfo', 'B'),
                          ('WindowLength', 'H'), ('QualityFactor', 'B'), ('Dcorr', 'b'), ('TravelTime', 'f'),
                          ('Reflectivity', 'f'), ('CleaningInfo', 'b'), ('Spare', 'B')])
    raw_dtype = np.dtype([('BeamPointingAngle', 'h'), ('TransmitSectorID', 'B'), ('DetectionInfo', 'B'),
                          ('WindowLength', 'H'), ('QualityFactor', 'B'), ('Dcorr', 'b'), ('TravelTime', 'f'),
                          ('Reflectivity', 'h'), ('CleaningInfo', 'b'), ('Spare', 'B')])
    conversions = {'BeamPointingAngle': 0.01,  # convert to degrees
                   'Reflectivity': 0.1,  # convert to dB
                   }

    def __init__(self, datablock, byteswap=False, read_limit=None):
        super(Data78_nrx, self).__init__(datablock, byteswap=byteswap,
                                         read_limit=read_limit)  # read as many records as passed in


class Data78(BaseData):
    """
    Raw range and angle datagram, aka 'N'/'4eh'/78d.  All data is contained
    in the header, rx, and tx arrays. the rx and tx arrays are ordered as in
    the data definition document, but have been converted to degrees, dB,
    meters, etc.
    The reported angles are in the transducer reference frame, so be careful of
    reverse mounted configurations. For the TX, forward angles are positive,
    for the RX angles to port are positive.
    """
    hdr_dtype = np.dtype([('Counter', 'H'), ('Serial#', 'H'), ('SoundSpeed', 'f'),
                          ('Ntx', 'H'), ('Nrx', 'H'), ('Nvalid', 'H'), ('SampleRate', 'f'), ('Dscale', 'I')])
    raw_dtype = np.dtype([('Counter', 'H'), ('Serial#', 'H'), ('SoundSpeed', 'H'),
                          ('Ntx', 'H'), ('Nrx', 'H'), ('Nvalid', 'H'), ('SampleRate', 'f'), ('Dscale', 'I')])
    conversions = {2: 0.1,  # sound speed to convert to meters/second
                   }

    def __init__(self, datablock, pingtime, byteswap=False):
        """Catches the binary datablock and decodes the first section and calls
        the decoder for the rest of the record."""
        super(Data78, self).__init__(datablock, byteswap=byteswap)
        self.time = pingtime
        self.read(datablock[self.hdr_sz:])

    def read(self, datablock):
        """Decodes the repeating parts of the record."""
        ntx = self.header[3]
        self.tx_data = Data78_ntx(datablock[:ntx * Data78_ntx.hdr_sz])
        self.tx = self.tx_data.header

        self.rx_data = Data78_nrx(datablock[ntx * Data78_ntx.hdr_sz:-1])
        self.rx = self.rx_data.header

        self.endchar = datablock[-1]

    def get_datablock(self, data=None):
        part1 = super(Data78, self).get_datablock()
        part2 = self.tx_data.get_datablock()
        part3 = self.rx_data.get_datablock()
        part4 = self.endchar
        return part1 + part2 + part3 + part4

    def get_rx_time(self):
        """
        Returns the receive times in POSIX time.
        """
        txnum = sorted(self.tx['TransmitSector#'])
        # deal with EM2040 in 200 kHz where the tx sector idx are [0,2]
        if txnum.max() == len(txnum):
            txnum[-1] = txnum[-1] - 1
        txdelays = self.tx['Delay'][txnum]
        rxdelays = txdelays[self.rx['TransmitSectorID']].astype(np.float64)
        rxtime = self.rx['TravelTime'].astype(np.float64) + rxdelays + self.pingtime
        return rxtime


class Data79(BaseData):
    """
    Quality factor datagram 4fh / 79d / 'O'.
    """
    hdr_dtype = np.dtype([('Counter', 'H'), ('SystemSerial#', 'H'),
                          ('Nrx', 'H'), ('Npar', 'H')])  # The data format has a Spare Byte here...
    qf_dtype = np.dtype([('QualityFactor', 'f4')])

    def __init__(self, datablock, POSIXtime, byteswap=False):
        """
        Catches the binary datablock and decodes the first section and calls
        the decoder for the rest of the record.
        """
        super(Data79, self).__init__(datablock, byteswap=byteswap)
        if self.header['Npar'] > 1:
            print("Warning: Datagram has expanded and may not parse correctly.")
        self.read(datablock[self.hdr_sz:-1])
        self.time = POSIXtime

    def read(self, datablock):
        """
        Reads the Quality Factor Datagram.
        """
        if self.header['Npar'] == 1:
            self.data = np.frombuffer(datablock, dtype=Data79.qf_dtype)
        else:
            print("Only parsing original IFREMER quality factor")
            step = 4 * self.header['Nrx'] * self.header['Npar']
            self.data = np.zeros(self.header['Nrx'], dtype=Data79.qf_dtype)
            for n in range(self.header['Nrx']):
                self.data = np.frombuffer(datablock[n * step:n * step + 4], dtype=Data79.qf_dtype)

    def get_datablock(self, data=None):
        raise Exception("Not Implemented")


class Data80_gga(BaseData):
    hdr_dtype = np.dtype([('MessageID', 'S5'), ('POSIX', 'd'),
                          ('Latitude', 'f'), ('LatDirection', 'S1'), ('Longitude', 'f'),
                          ('LonDirection', 'S1'), ('GPSQuality', 'B'), ('#SV', 'B'), ('HDOP', 'f'),
                          ('OrthometricHeight', 'f'), ('HeightUnits', 'S1'), ('GeoidSeparation', 'f'),
                          ('SeparationUnits', 'S1'), ('AgeOfDGPS', 'f'), ('ReferenceStationID', 'H'),
                          ('CheckSum', 'H')])

    def __init__(self, datablock, byteswap=False, read_limit=None):
        # don't call the base init as this is not a packed binary array but text data instead
        self.header = np.zeros(1, dtype=self.hdr_dtype)[0]
        temp = datablock.split(b',')
        for n, t in enumerate(temp):
            if len(t) > 0:
                if n == 0 or n == 3 or n == 5 or n == 10 or n == 12:
                    self.header[n] = t.decode()
                elif n == 1 or n == 8 or n == 9 or n == 11 or n == 13:
                    self.header[n] = float(t)
                elif n == 2:
                    deg = int(t[:2])
                    minutes = float(t[2:])
                    self.header[n] = deg + minutes / 60.
                elif n == 4:
                    deg = int(t[:3])
                    minutes = float(t[3:])
                    self.header[n] = deg + minutes / 60.
                elif n == 14:
                    t2 = t.split(b'*')
                    self.header[-2] = int(t2[0])
                    self.header[-1] = int(t2[1], 16)
                else:
                    self.header[n] = int(t)
            else:
                self.header[n] = None

    def get_datablock(self, data=None):
        raise Exception("Writing of GGA not supported yet.")


class Data80_ggk(BaseData):
    hdr_dtype = np.dtype([('MessageID', 'S5'), ('UTCTime', 'd'), ('UTCDay', 'S6'),
                          ('Latitude', 'f'), ('LatDirection', 'S1'), ('Longitude', 'f'),
                          ('LonDirection', 'S1'), ('GPSQuality', 'B'), ('#SV', 'B'), ('DOP', 'f'),
                          ('EllipsoidHeight', 'f'), ('HeightUnits', 'S1'), ('CheckSum', 'H')])

    def __init__(self, datablock, byteswap=False, read_limit=None):
        # don't call the base init as this is not a packed binary array but text data instead
        self.header = np.zeros(1, dtype=self.hdr_dtype)[0]
        temp = datablock.split(b',')
        for n, t in enumerate(temp):
            if len(t) > 0:
                if n == 0 or n == 2 or n == 4 or n == 6:
                    self.header[n] = t.decode()
                elif n == 1 or n == 9:
                    self.header[n] = float(t)
                elif n == 3:
                    deg = int(t[:2])
                    minutes = float(t[2:])
                    self.header[n] = deg + minutes / 60.
                elif n == 5:
                    deg = int(t[:3])
                    minutes = float(t[3:])
                    self.header[n] = deg + minutes / 60.
                elif n == 10:
                    self.header[n] = float(t[3:])
                elif n == 11:
                    t2 = t.split(b'*')
                    self.header[n] = t2[0].decode()
                    self.header[n + 1] = int(t2[1], 16)
                else:
                    self.header[n] = int(t)
            else:
                self.header[n] = None

    def get_datablock(self, data=None):
        raise Exception("Writing of GGK not supported yet.")


class Data80(BaseData):
    """
    Position datagram, 0x50 / 'P' / 80. Available data is in the header
    list, and all data has been converted to degrees or meters.
    """

    hdr_dtype = np.dtype([('Counter', 'H'), ('Serial#', 'H'), ('Latitude', 'd'),
                          ('Longitude', 'd'), ('Quality', 'f'), ('Speed', 'f'), ('Course', 'f'),
                          ('Heading', 'f'), ('System', 'B'), ('NumberInputBytes', 'B')])
    raw_dtype = np.dtype([('Counter', 'H'), ('Serial#', 'H'), ('Latitude', 'i'),
                          ('Longitude', 'i'), ('Quality', 'H'), ('Speed', 'H'), ('Course', 'H'),
                          ('Heading', 'H'), ('System', 'B'), ('NumberInputBytes', 'B')])

    def __init__(self, datablock, POSIXtime, byteswap=False):
        """Catches the binary datablock and decodes the record."""
        super(Data80, self).__init__(datablock, byteswap=byteswap)
        # read the original datagram, of which the size is the last part of the header.
        self.raw_data = datablock[self.hdr_sz:self.hdr_sz + self.header[-1]]
        self.header['Latitude'] /= 20000000.  # convert to degrees
        self.header['Longitude'] /= 10000000.  # convert to degrees
        self.header['Quality'] *= 0.01  # convert to meters
        self.header['Speed'] *= 0.01  # convert to meters/second
        self.header['Course'] *= 0.01  # convert to degrees
        self.header['Heading'] *= 0.01  # convert to degrees
        self.time = POSIXtime
        self.parse_raw()

    def parse_raw(self):
        """
        Parses the raw_data that arrived in SIS and puts it in source_data.
        """
        msg_type = np.frombuffer(self.raw_data[:5], dtype='S5')
        if msg_type[0] == b'INGGA':
            self._parse_gga()
        elif msg_type[0] == b'GPGGA':
            self._parse_gga()
        elif msg_type[0] == b'INGGK':
            self._parse_ggk()
        elif msg_type[0] == b'GPGGK':
            self._parse_ggk()

    def _parse_gga(self):
        """
        parse the gga string.
        """
        self.gg_data = Data80_gga(self.raw_data)
        self.source_data = self.gg_data.header  # for backward compatibility

    def _parse_ggk(self):
        """
        parse the ggk string.
        """
        self.gg_data = Data80_ggk(self.raw_data)
        self.source_data = self.gg_data.header  # for backward compatibility

    def get_datablock(self, data=None):
        raise Exception("Not Implemented")

    def get_display_string(self):
        s = super(Data80, self).get_display_string()
        # self.parse_raw()
        s += '\n***raw data record***\n'
        s += self.gg_data.get_display_string()
        return s


class Data82(BaseData):
    """
    Runtime parameters datagram, 0x52 / 'R' / 82.
    Values that are converted into whole units include: AbsorptionCoefficent,
    TransmitPulseLength, TransmitBeamwidth, ReceiveBeamwidth, and
    TransmitAlongTilt.
    """

    hdr_dtype = np.dtype([('Counter', 'H'), ('SystemSerial#', 'H'),
                          ('OperatorStationStatus', 'B'), ('ProcessingUnitStatus', 'B'),
                          ('BSPStatus', 'B'), ('SonarHeadOrTransceiverStatus', 'B'),
                          ('Mode', 'B'), ('FilterID', 'B'), ('MinDepth', 'H'), ('MaxDepth', 'H'),
                          ('AbsorptionCoefficent', 'f'), ('TransmitPulseLength', 'f'),
                          ('TransmitBeamWidth', 'f'), ('TransmitPower', 'b'),
                          ('ReceiveBeamWidth', 'f'), ('ReceiveBandWidth50Hz', 'B'),
                          ('ReceiverFixedGain', 'B'), ('TVGlawCrossoverAngle', 'B'),
                          ('SourceOfSoundSpeed', 'B'), ('MaxPortSwathWidth', 'H'),
                          ('BeamSpacing', 'B'), ('MaxPortCoverage', 'B'),
                          ('YawAndPitchStabilization', 'B'), ('MaxStarboardCoverage', 'B'),
                          ('MaxStarboardSwathWidth', 'H'), ('TransmitAlongTilt', 'f'),
                          ('HiLoFrequencyAbsorptionCoeffRatio', 'B')])
    raw_dtype = np.dtype([('Counter', 'H'), ('SystemSerial#', 'H'),
                          ('OperatorStationStatus', 'B'), ('ProcessingUnitStatus', 'B'),
                          ('BSPStatus', 'B'), ('SonarHeadOrTransceiverStatus', 'B'),
                          ('Mode', 'B'), ('FilterID', 'B'), ('MinDepth', 'H'), ('MaxDepth', 'H'),
                          ('AbsorptionCoefficent', 'H'), ('TransmitPulseLength', 'H'),
                          ('TransmitBeamWidth', 'H'), ('TransmitPower', 'b'),
                          ('ReceiveBeamWidth', 'B'), ('ReceiveBandWidth50Hz', 'B'),
                          ('ReceiverFixedGain', 'B'), ('TVGlawCrossoverAngle', 'B'),
                          ('SourceOfSoundSpeed', 'B'), ('MaxPortSwathWidth', 'H'),
                          ('BeamSpacing', 'B'), ('MaxPortCoverage', 'B'),
                          ('YawAndPitchStabilization', 'B'), ('MaxStarboardCoverage', 'B'),
                          ('MaxStarboardSwathWidth', 'H'), ('TransmitAlongTilt', 'h'),
                          ('HiLoFrequencyAbsorptionCoeffRatio', 'B')])
    conversions = {'AbsorptionCoefficent': 0.01, 'TransmitPulseLength': 0.000001,
                   'TransmitBeamWidth': 0.1, 'ReceiveBeamWidth': 0.1, 'TransmitAlongTilt': 0.1,
                   }

    def __init__(self, datablock, POSIXtime, byteswap=False):
        """Catches the binary datablock and decodes the record."""
        super(Data82, self).__init__(datablock, byteswap=byteswap)
        self.time = POSIXtime

    def repr_byte(self, field_number):
        """
        Prints the given 1 bite field in a binary form.
        """
        if isinstance(self.header[field_number], np.uint8):
            return str(np.binary_repr(self.header[field_number], width=8))

    def print_byte(self, field_number):
        print(self.repr_byte(field_number))

    def get_display_string(self):
        """
        Displays contents of the header to the command window.
        """
        s = ""
        bitfields = np.array([2, 3, 4, 5, 6, 7, 18, 20, 22, 26])
        for n, name in enumerate(self.header.dtype.names):
            if np.any(bitfields == n):
                s += name + ' : ' + np.binary_repr(self.header[n], width=8) + "\n"
            else:
                s += name + ' : ' + str(self.header[n]) + "\n"
        return s


class Data83(BaseData):
    """
    Seabed Imagary datagram 053h / 83d / 'Seabed image data'.  All data is
    converted into whole units of degrees, meters, dB, etc, except Oblique
    Backscatter and Normal Backscatter which are in their raw form.
    """
    hdr_dtype = np.dtype([('PingCounter', 'H'), ('SystemSerial#', 'H'),
                          ('MeanAbsorption', "f"), ('PulseLength', "f"), ('RangeToNormal', 'H'),
                          ('StartRangeSampleOfTVG', 'H'), ('StopRangeSampleOfTVG', 'H'),
                          ('NormalIncidenceBS', "f"), ('ObliqueBS', "f"), ('TxBeamwidth', "f"),
                          ('TVGLawCrossoverAngle', "f"), ('NumberValidBeams', 'B')])
    raw_dtype = np.dtype([('PingCounter', 'H'), ('SystemSerial#', 'H'),
                          ('MeanAbsorption', "H"), ('PulseLength', "H"), ('RangeToNormal', 'H'),
                          ('StartRangeSampleOfTVG', 'H'), ('StopRangeSampleOfTVG', 'H'),
                          ('NormalIncidenceBS', "b"), ('ObliqueBS', "b"), ('TxBeamwidth', "H"),
                          ('TVGLawCrossoverAngle', "B"), ('NumberValidBeams', 'B')])
    conversions = {2: 0.01, 3: 10 ** -6,
                   7: 1,  # FIXME: check this
                   8: 1,  # FIXME: check this
                   9: 0.1, 10: 0.1,
                   }
    beaminfo_dtype = np.dtype([('BeamIndexNumber', 'B'), ('SortingDirection', 'b'),
                               ('#SamplesPerBeam', 'H'), ('CenterSample#', 'H')])

    def __init__(self, datablock, byteswap=False):
        """
        Catches the binary datablock and decodes the first section and calls
        the decoder for the rest of the record.
        """
        super(Data83, self).__init__(datablock, byteswap=byteswap)
        numbeams = self.header[-1]

        self._read(datablock[self.hdr_sz:], numbeams)

    def _read(self, datablock, numbeams):
        """
        Reads the data section of the record.
        """
        beaminfo_sz = Data83.beaminfo_dtype.itemsize
        samples_dtype = np.dtype([('Amplitude', "b")])
        samples_sz = samples_dtype.itemsize
        p = beaminfo_sz * numbeams
        self.beaminfo = np.frombuffer(datablock[:p], dtype=Data83.beaminfo_dtype)
        maxsamples = self.beaminfo['#SamplesPerBeam'].max()
        self.samples = np.zeros((numbeams, maxsamples), dtype='float')
        for n in range(numbeams):
            numsamples = self.beaminfo[n]['#SamplesPerBeam']
            temp = np.frombuffer(datablock[p:p + numsamples * samples_sz], dtype=samples_dtype)
            p += numsamples * samples_sz
            # startsample = self.beaminfo[n]['CenterSample#']
            self.samples[n, :numsamples] = temp.astype('float')[:]
        self.samples *= 0.5  # FIXME: check this

    def get_datablock(self, data=None):
        raise Exception("Not Implemented")


class Data85_soundspeed(BaseData):
    hdr_dtype = np.dtype([('Depth', 'd'), ('SoundSpeed', 'f')])
    raw_dtype = np.dtype([('Depth', 'I'), ('SoundSpeed', 'I')])
    conversions = {'SoundSpeed': 0.1}

    def __init__(self, datablock, depth_resolution, byteswap=False, read_limit=None):
        # add the depth resolution to just the current class instance rather than all occurances of Data85
        self.conversions = {'Depth': depth_resolution}
        self.conversions.update(Data85_soundspeed.conversions)
        super(Data85_soundspeed, self).__init__(datablock, byteswap=byteswap,
                                                read_limit=read_limit)  # read as many records as passed in
        self.depth_resolution = depth_resolution

    def plot(self):
        """
        Creates a simple plot of the cast.
        """
        plt.figure()
        plt.plot(self.data['SoundSpeed'], self.data['Depth'])
        plt.ylim((self.data['Depth'].max(), self.data['Depth'].min()))
        plt.xlabel('Sound Speed (m/s)')
        plt.ylabel('Depth (m)')
        plt.title('Cast at POSIX time ' + str(self.header['Time']))
        plt.draw()


class Data85(BaseData):
    """
    Sound Speed datagram 055h / 85d / 'U'. Time is in POSIX, depth
    is in meters, sound speed is in meters per second.
    """
    hdr_dtype = np.dtype([('ProfileCounter', 'H'), ('SystemSerial#', 'H'),
                          ('Date', 'I'), ('Time', "d"), ('NumEntries', 'H'), ('DepthResolution', 'H')])
    raw_dtype = np.dtype([('ProfileCounter', 'H'), ('SystemSerial#', 'H'),
                          ('Date', 'I'), ('Time', "I"), ('NumEntries', 'H'),
                          ('DepthResolution', 'H')])

    def __init__(self, datablock, byteswap=False):
        """
        Catches the binary datablock and decodes the first section and calls
        the decoder for the rest of the record.
        """
        super(Data85, self).__init__(datablock, byteswap=byteswap)
        self.time = self._maketime(self.header['Date'], self.header['Time'])
        depth_resolution = self.header['DepthResolution'] * 0.01

        self.ss = Data85_soundspeed(datablock[self.hdr_sz:-1], depth_resolution)
        self.last_byte = datablock[-1]
        self.data = self.ss.header  # backward compatibility

    def get_datablock(self, data=None):
        # FIXME: Not sure what happens if TVG was removed
        part1 = super(Data85, self).get_datablock()
        part2 = self.ss.get_datablock()
        return part1 + part2 + self.last_byte

    def _maketime(self, date, time):
        """
        Makes the time stamp of the current packet as a POSIX time stamp.
        UTC is assumed.
        """
        date = str(date)
        year = int(date[:4])
        month = int(date[4:6])
        day = int(date[6:])
        numdays = dtm.date(year, month, day).toordinal() - dtm.date(1970, 1, 1).toordinal()
        dayseconds = time  # * 0.001
        return numdays * 24 * 60 * 60 + dayseconds

    def plot(self):
        """
        Creates a simple plot of the cast.
        """
        self.ss.plot()

    def get_display_string(self):
        """
        Displays contents of the header to the command window.
        """
        s = super(Data85, self).get_display_string()
        s += "\n"
        s += "POSIXtime : " + str(self.time) + "\n"
        return s


class Data88_xyz(BaseData):
    hdr_dtype = np.dtype([('Depth', 'f'), ('AcrossTrack', 'f'), ('AlongTrack', 'f'),
                          ('WindowLength', 'H'), ('QualityFactor', 'B'), ('IncidenceAngleAdjustment', 'f'),
                          ('Detection', 'B'), ('Cleaning', 'b'), ('Reflectivity', 'f')])
    raw_dtype = np.dtype([('Depth', 'f'), ('AcrossTrack', 'f'), ('AlongTrack', 'f'),
                          ('WindowLength', 'H'), ('QualityFactor', 'B'), ('IncidenceAngleAdjustment', 'b'),
                          ('Detection', 'B'), ('Cleaning', 'b'), ('Reflectivity', 'h')])
    conversions = {'IncidenceAngleAdjustment': 0.1,  # convert to degrees
                   'Reflectivity': 0.1,  # convert to dB
                   }

    def __init__(self, datablock, byteswap=False, read_limit=None):
        super(Data88_xyz, self).__init__(datablock, byteswap=byteswap,
                                         read_limit=read_limit)  # read as many records as passed in


class Data88(BaseData):
    """
    XYZ datagram, 0x58 / 'X' / 88.  All data is in the header list or
    stored in the 'data' array.  Values have been converted to degrees and
    dB.
    """
    hdr_dtype = np.dtype([('Counter', 'H'), ('Serial#', 'H'), ('Heading', 'f'),
                          ('SoundSpeed', 'f'), ('TransmitDepth', 'f'), ('NumBeams', 'H'),
                          ('NumValid', 'H'), ('SampleFrequency', 'f'), ('Spare', 'i')])
    raw_dtype = np.dtype([('Counter', 'H'), ('Serial#', 'H'), ('Heading', 'H'),
                          ('SoundSpeed', 'H'), ('TransmitDepth', 'f'), ('NumBeams', 'H'),
                          ('NumValid', 'H'), ('SampleFrequency', 'f'), ('Spare', 'i')])
    conversions = {'Heading': 0.01,  # convert to degrees
                   'SoundSpeed': 0.1,  # convert to m/s
                   }

    def __init__(self, datablock, byteswap=False):
        """Catches the binary datablock and decodes the first section and calls
        the decoder for the rest of the record."""
        super(Data88, self).__init__(datablock, byteswap=byteswap)

        self.read(datablock[self.hdr_sz:])  # calling this way to maintain backward compatibility

    def read(self, datablock):
        """
        Reads the data section of the record.
        """
        # buffer length goes to -1 because of the uint8 buffer before etx
        self.xyz = Data88_xyz(datablock[:-1])
        self.last_byte = datablock[-1]
        self.data = self.xyz.header

    def get_datablock(self, data=None):
        # FIXME: missing one character at the end??  See the read function
        part1 = super(Data88, self).get_datablock()
        part2 = self.xyz.get_datablock()
        return part1 + part2 + self.last_byte


class Data89_beaminfo(BaseData):
    hdr_dtype = np.dtype([('SortingDirection', 'b'), ('DetectionInfo', 'B'),
                          ('#SamplesPerBeam', 'H'), ('CenterSample#', 'H')])

    def __init__(self, datablock, byteswap=False, read_limit=None):
        super(Data89_beaminfo, self).__init__(datablock, byteswap=byteswap,
                                              read_limit=read_limit)  # read as many records as passed in


class Data89_samples(BaseData):
    hdr_dtype = np.dtype([('Amplitude', "f2")])  # FIXME: float16 is right type?
    raw_dtype = np.dtype([('Amplitude', "h")])
    conversions = {'Amplitude': 0.1,
                   }

    def __init__(self, datablock, byteswap=False, read_limit=None):
        super(Data89_samples, self).__init__(datablock, byteswap=byteswap,
                                             read_limit=read_limit)  # read as many records as passed in


class Data89(BaseData):
    """
    Seabed Image datagram 059h / 89d / 'Y'.
    """
    hdr_dtype = np.dtype([('Counter', 'H'), ('SystemSerial#', 'H'),
                          ('SamplingFreq', 'f'), ('RangeToNormal', 'H'), ('NormalBackscatter', "f"),
                          ('ObliqueBackscatter', "f"), ('TXBeamWidth', "f"), ('TVGCrossover', "f"),
                          ('NumberValidBeams', 'H')])
    raw_dtype = np.dtype([('Counter', 'H'), ('SystemSerial#', 'H'),
                          ('SamplingFreq', 'f'), ('RangeToNormal', 'H'), ('NormalBackscatter', "h"),
                          ('ObliqueBackscatter', "h"), ('TXBeamWidth', "H"), ('TVGCrossover', "H"),
                          ('NumberValidBeams', 'H')])
    conversions = {'NormalBackscatter': 0.1, 'ObliqueBackscatter': 0.1,
                   'TXBeamWidth': 0.1, 'TVGCrossover': 0.1,
                   }

    def __init__(self, datablock, POSIXtime, byteswap=False):
        """Catches the binary datablock and decodes the first section and calls
        the decoder for the rest of the record."""
        super(Data89, self).__init__(datablock, byteswap=byteswap)
        numbeams = self.header[-1]
        self.time = POSIXtime

        self._read(datablock[self.hdr_sz:], numbeams)

    def _read(self, datablock, numbeams):
        """
        Reads the data section of the record. Backscatter is stored in one long
        array.  Use the included carve method to reshape the time series data
        into an array. Note the existance of the beam_position array that
        points to the start of each array.
        """
        samples_dtype = np.dtype([('Amplitude', "h")])
        self.beaminfo_data = Data89_beaminfo(datablock, read_limit=numbeams)
        self.beaminfo = self.beaminfo_data.header
        p = Data89_beaminfo.hdr_sz * numbeams
        t = self.beaminfo['#SamplesPerBeam'].sum()
        self.samples_data = Data89_samples(datablock[p:], read_limit=t)
        self.samples = self.samples_data.Amplitude

        self.beam_position = np.zeros(self.beaminfo['#SamplesPerBeam'].shape, dtype=np.uint32)
        for n in range(len(self.beam_position) - 1):
            self.beam_position[n + 1] = self.beaminfo['#SamplesPerBeam'][n] + self.beam_position[n]

    def get_datablock(self, data=None):
        # FIXME: Not sure what happens if reshape is called
        part1 = super(Data89, self).get_datablock()
        part2 = self.beaminfo_data.get_datablock()
        part3 = self.samples_data.get_datablock()
        return part1 + part2 + part3

    def reshape(self):
        """
        Reshapes the samples array and carves it into chunks. A 2D array is
        returned.
        """
        numbeams = self.header[-1]
        c = self.beaminfo['CenterSample#']
        t = self.beaminfo['#SamplesPerBeam']
        s = self.beaminfo['SortingDirection']
        # figure the array size
        bottom = t - c
        top = c.copy()
        idx = np.nonzero(s < 0)[0]
        top[idx] = bottom[idx]
        bottom[idx] = c[idx]
        maxsamples = top.max() + bottom.max()
        self.samplearray = np.zeros((maxsamples, numbeams), dtype=np.float16)
        self.samplearray[:] = np.nan
        centerpos = top.max()
        for n in range(len(self.beaminfo)):
            if t[n] > 0:
                pointer = self.beam_position[n]
                beamsamples = self.samples[pointer:pointer + t[n]]
                start = centerpos - top[n]
                self.samplearray[start:start + t[n], n] = beamsamples[::s[n]]
        return self.samplearray, centerpos

    def center(self):
        """
        Returns the center sample, which is at the bottom detection.
        """
        # at times the outer most beam has overflowed the max index.
        # this leads me to believe that the center sample counts with the first
        # sample in each beam, so the center is the start + the center count -1
        # GAR 20150127
        idx = self.beam_position + self.beaminfo['CenterSample#'] - 1
        center = self.samples[idx]
        sidx = np.nonzero(self.beaminfo['SortingDirection'] == -1)[0]
        idx = self.beam_position[sidx + 1] - self.beaminfo['CenterSample#'][sidx]
        center[sidx] = self.samples[idx]
        return center

    def plot(self):
        """
        Plots the output from the "center" method.
        """
        data, cidx = self.center()
        # beams = range(self.header[-1])
        # samples = range(-1*cidx, len(data)-cidx)
        # X,Y = np.meshgrid(beams, samples)
        # plt.pcolormesh(X,Y,data, cmap = 'gray')
        plt.imshow(data, aspect='auto', cmap='gray', interpolation='none')
        plt.clim((-80, 0))


class Data102_nrx(BaseData):
    hdr_dtype = np.dtype([('BeamPointingAngle', "f"), ('Range', "f"),
                          ('TransmitSectorID', 'B'), ('Reflectivity', "f"), ('QualityFactor', 'B'),
                          ('DetectionWindowLength', 'B'), ('BeamNumber', 'h'), ('Spare', 'H')])
    raw_dtype = np.dtype([('BeamPointingAngle', "h"), ('Range', "H"),
                          ('TransmitSectorID', 'B'), ('Reflectivity', "b"), ('QualityFactor', 'B'),
                          ('DetectionWindowLength', 'B'), ('BeamNumber', 'h'), ('Spare', 'H')])
    conversions = {'BeamPointingAngle': 0.01, 'Range': 0.25, 'Reflectivity': 0.5}

    def __init__(self, datablock, byteswap=False, read_limit=None):
        super(Data102_nrx, self).__init__(datablock, byteswap=byteswap,
                                          read_limit=read_limit)  # read as many records as passed in


class Data102_ntx(BaseData):
    hdr_dtype = np.dtype([('TiltAngle', "f"), ('FocusRange', "f"),
                          ('SignalLength', "f"), ('Delay', "f"),
                          ('CenterFrequency', 'I'), ('Bandwidth', "I"), ('SignalWaveformID', 'B'),
                          ('TransmitSector#', 'B')])
    raw_dtype = np.dtype([('TiltAngle', "h"), ('FocusRange', "H"),
                          ('SignalLength', "I"), ('Delay', "I"),
                          ('CenterFrequency', 'I'), ('Bandwidth', "H"), ('SignalWaveformID', 'B'),
                          ('TransmitSector#', 'B')])
    conversions = {'TiltAngle': 0.01, 'FocusRange': 0.1,
                   'SignalLength': 10 ** -6, 'Delay': 10 ** -6,
                   'Bandwidth': 10}

    def __init__(self, datablock, byteswap=False, read_limit=None):
        super(Data102_ntx, self).__init__(datablock, byteswap=byteswap,
                                          read_limit=read_limit)  # read as many records as passed in


class Data102(BaseData):
    """
    Range and angle datagram, 66h / 102 / 'f'.  All values are converted to
    whole units, meaning meters, seconds, degrees, Hz, etc.
    """
    hdr_dtype = np.dtype([('PingCounter', 'H'), ('SystemSerial#', 'H'),
                          ('Ntx', 'H'), ('Nrx', 'H'), ('SamplingFrequency', "f"), ('Depth', "f"),
                          ('SoundSpeed', "f"), ('MaximumBeams', 'H'), ('Spare1', 'H'), ('Spare2', 'H')])
    raw_dtype = np.dtype([('PingCounter', 'H'), ('SystemSerial#', 'H'),
                          ('Ntx', 'H'), ('Nrx', 'H'), ('SamplingFrequency', "I"), ('Depth', "i"),
                          ('SoundSpeed', "H"), ('MaximumBeams', 'H'), ('Spare1', 'H'),
                          ('Spare2', 'H')])
    conversions = {'SoundSpeed': 0.1, 'SamplingFrequency': 0.01, 'Depth': 0.01}

    def __init__(self, datablock, byteswap=False):
        """
        Catches the binary datablock and decodes the first section and calls
        the decoder for the rest of the record.
        """
        super(Data102, self).__init__(datablock, byteswap=byteswap)

        self.read(datablock[self.hdr_sz:-1])

    def read(self, datablock):
        """
        Reads the data section of the record and converts values to whole
        units.
        """
        ntx = self.header['Ntx']
        nrx = self.header['Nrx']
        # read ntx
        self.tx_data = Data102_ntx(datablock, read_limit=ntx)
        self.tx = self.tx_data.header  # maintain backward compatibility

        # read nrx -- skip over the tx data in the block
        self.rx_data = Data102_nrx(datablock[ntx * self.tx_data.hdr_sz:], read_limit=nrx)
        self.rx = self.rx_data.header  # maintain backward compatibility

    def get_datablock(self, data=None):
        # FIXME: Not sure what happens if TVG was removed
        part1 = super(Data102, self).get_datablock()
        part2 = self.tx_data.get_datablock()
        part3 = self.rx_data.get_datablock()
        return part1 + part2 + part3


class Data104(BaseData):
    """
    Depth (pressure) or height datagram, 0x68h / 'h' / 104.  Height information
    is converted to meters.
    """
    hdr_dtype = np.dtype([('Counter', 'H'), ('SystemSerial#', 'H'),
                          ('Height', "f"), ('HeightType', 'B')])
    raw_dtype = np.dtype([('Counter', 'H'), ('SystemSerial#', 'H'),
                          ('Height', "i"), ('HeightType', 'B')])
    conversions = {'Height': 0.01}

    def __init__(self, datablock, byteswap=False):
        """Catches the binary datablock and decodes the record."""
        super(Data104, self).__init__(datablock, byteswap=byteswap)


class Data107_nrx(BaseData):
    """This class is a bit different since the data is intertwined.  The datagrams 
    are written header followed by samples where the number of samples is in the header."""
    hdr_dtype = np.dtype([('BeamPointingAngle', "f"), ('StartRangeSample#', 'H'),
                          ('NumberSamples', 'H'), ('DetectedRange', 'H'), ('TransmitSector#', 'B'),
                          ('Beam#', 'B')])
    raw_dtype = np.dtype([('BeamPointingAngle', "h"),
                          ('StartRangeSample#', 'H'), ('NumberSamples', 'H'),
                          ('DetectedRange', 'H'), ('TransmitSector#', 'B'),
                          ('Beam#', 'B')])

    def __init__(self, datablock, byteswap=False, read_limit=None):
        # declare rx stuff
        if read_limit is None:
            raise Exception("Must specify a number of rx datagrams to read")
        p = 0  # pointer to where we are in the datablock
        nrx_sz = self.hdr_size
        nrx = read_limit
        self.header = np.zeros(nrx, dtype=self.raw_dtype)

        # declare amplitudes stuff
        amp_dtype = np.dtype([('SampleAmplitude', "b")])
        numamp = len(datablock) - nrx_sz * nrx  # figures out the total number of amplitudes in the datablock
        # Initialize array to NANs. Source:http://stackoverflow.com/a/1704853/1982894
        tempamp = np.empty(numamp, dtype=amp_dtype)
        tempamp[:] = np.NAN
        # get the rx and amplitude data
        pamp = 0
        for n in range(nrx):
            self.header[n] = np.frombuffer(datablock[p:p + nrx_sz], dtype=self.raw_dtype)
            p += nrx_sz
            # the number of samples for this beam
            beamsz = self.header[n][2]
            tempamp[pamp:pamp + beamsz] = np.frombuffer(datablock[p:p + beamsz], dtype=amp_dtype)
            p += beamsz
            pamp += beamsz
        self.header = self.header.astype(self.hdr_dtype)
        self.header['BeamPointingAngle'] *= 0.01
        # unwined the beam data into an array
        numsamples = self.header['NumberSamples']
        self.ampdata = np.empty((numsamples.max(), nrx), dtype=np.float32)
        self.ampdata[:] = np.NAN
        pamp = 0
        for n in range(nrx):
            self.ampdata[:numsamples[n], n] = 0.5 * tempamp[pamp:pamp + numsamples[n]].astype(np.float32)
            pamp += numsamples[n]

    def get_datablock(self, data=None):
        raise Exception("Not implemented")


class Data107_ntx(BaseData):
    hdr_dtype = np.dtype([('TiltTx', "f"), ('CenterFrequency', "I"),
                          ('TransmitSector#', 'B'), ('Spare', 'B')])
    raw_dtype = np.dtype([('TiltTx', "h"), ('CenterFrequency', "H"),
                          ('TransmitSector#', 'B'), ('Spare', 'B')])
    conversions = {'TiltTx': 0.01, 'CenterFrequency': 10}

    def __init__(self, datablock, byteswap=False, read_limit=None):
        super(Data107_ntx, self).__init__(datablock, byteswap=byteswap,
                                          read_limit=read_limit)  # read as many records as passed in


class Data107(BaseData):
    """
    The water column datagram, 6Bh / 107d / 'k'.  The receiver beams are roll
    stabilized.  Units have been shifted to whole units as in hertz, meters, 
    seconds, etc.  Watercolumn data is in ampdata as 0.5 dB steps.
    """
    hdr_dtype = np.dtype([('PingCounter', 'H'), ('SystemSerial#', 'H'),
                          ('#OfDatagrams', 'H'), ('Datagram#', 'H'), ('#TxSectors', 'H'),
                          ('Total#Beams', 'H'), ('NumberBeamsInDatagram', 'H'), ('SoundSpeed', "f"),
                          ('SamplingFrequency', "d"), ('TxHeave', "f"), ('TVGfunction', 'B'),
                          ('TVGoffset', 'b'), ('ScanningInfo', 'B'), ('Spare', '3B')])
    raw_dtype = np.dtype([('PingCounter', 'H'), ('SystemSerial#', 'H'),
                          ('#OfDatagrams', 'H'), ('Datagram#', 'H'), ('#TxSectors', 'H'),
                          ('Total#Beams', 'H'), ('NumberBeamsInDatagram', 'H'), ('SoundSpeed', "H"),
                          ('SamplingFrequency', "I"), ('TxHeave', "h"), ('TVGfunction', 'B'),
                          ('TVGoffset', 'b'), ('ScanningInfo', 'B'), ('Spare', '3B')])
    conversions = {'SoundSpeed': 0.1, 'SamplingFrequency': 0.01, 'TxHeave': 0.01}

    def __init__(self, datablock, byteswap=False):
        """
        Catches the binary datablock and decodes the first section and calls
        the decoder for the rest of the record.
        """
        super(Data107, self).__init__(datablock, byteswap=byteswap)
        self.hasTVG = True

        self.read(datablock[self.hdr_sz:])

    def read(self, datablock):
        """
        Reads the variable section of the datagram.
        """
        # declare tx stuff
        ntx = self.header[4]
        # get the tx data
        self.tx_data = Data107_ntx(datablock, read_limit=ntx)
        self.tx = self.tx_data.header
        p = ntx * self.tx_data.hdr_sz
        nrx = self.header[6]
        self.rx_data = Data107_nrx(datablock[p:], read_limit=nrx)
        self.rx = self.rx_data.header
        self.ampdata = self.rx_data.ampdata

    def get_datablock(self, data=None):
        # FIXME: Not sure what happens if TVG was removed
        part1 = super(Data107, self).get_datablock()
        part2 = self.tx_data.get_datablock()
        part3 = self.rx_data.get_datablock()
        return part1 + part2 + part3

    def deTVG(self, absorption, OFS, usec=True):
        """
        Removes the TVG function from the ampdata.  The TVG will be removed
        only if the hasTVG flag is set to True. A value for Alpha and OFS need
        to be provided since they do not exist in the water column datagram.
        The TVG function removed (from the datagram definition) is
        func_TVG = X * log(R) + 2 * Absorption * R + OFS + C
        Set the kwarg 'usec' to False to avoid applying the header c value.

        Absorption should be supplied in dB / m.
        """
        x = self.header['TVGfunction']
        if usec:
            c = self.header['TVGoffset']
        else:
            c = 0
        s = self.header['SoundSpeed']
        dt = self.header['SamplingFrequency']
        r = np.arange(len(self.ampdata)) * s / (2 * dt)
        f = x * np.log10(r) + 2 * absorption * r / 1000. + OFS + c
        f[0] = OFS + c
        f.shape = (len(f), -1)
        self.ampdata -= f
        self.hasTVG = False

    def plot(self):
        """
        Plots the watercolumn data.
        """
        a = self.rx['BeamPointingAngle']
        r = np.arange(len(self.ampdata))
        A, R = np.meshgrid(a, r)
        # swap sides through -1 to make the negative angle be the positive direction
        X = -1 * R * np.sin(np.deg2rad(A))
        Y = R * np.cos(np.deg2rad(A))
        plt.figure()
        im = plt.pcolormesh(X, Y, self.ampdata)
        plt.ylim((r.max(), 0))
        c = plt.colorbar()
        c.set_label('dB re $1\mu Pa$ at 1 meter')
        plt.xlabel('Across Track (meters)')
        plt.ylabel('Depth (meters)')
        cstd = np.nanstd(self.ampdata)
        cmean = np.nanmean(self.ampdata)
        im.set_clim((cmean - 3 * cstd, cmean + 3 * cstd))
        plt.grid()
        plt.draw()


class Data109(BaseData):
    """
    The Stave Data Datagram, 6Dh / 109d / 'm'.  This data definition does not
    exist in the normal documentation.  All values are converted to whole
    units.
    """
    hdr_dtype = np.dtype([('PingCounter', 'H'), ('SystemSerial#', 'H'),
                          ('#Datagrams', 'H'), ('Datagram#', 'H'), ('RxSamplingFrequency', "f"),
                          ('SoundSpeed', "f"), ('StartRangeRefTx', 'H'), ('TotalSample', 'H'),
                          ('#SamplesInDatagram', 'H'), ('Stave#', 'H'), ('#StavesPerSample', 'H'),
                          ('RangeToNormal', 'H'), ('Spare', 'H')])
    raw_dtype = np.dtype([('PingCounter', 'H'), ('SystemSerial#', 'H'),
                          ('#Datagrams', 'H'), ('Datagram#', 'H'), ('RxSamplingFrequency', "I"),
                          ('SoundSpeed', "H"), ('StartRangeRefTx', 'H'), ('TotalSample', 'H'),
                          ('#SamplesInDatagram', 'H'), ('Stave#', 'H'), ('#StavesPerSample', 'H'),
                          ('RangeToNormal', 'H'), ('Spare', 'H')])
    conversions = {'RxSamplingFrequency': 0.01, 'SoundSpeed': 0.1}

    def __init__(self, datablock, byteswap=False):
        """
        Catches the binary datablock and decodes the first section and calls
        the decoder for the rest of the record.
        """
        super(Data109, self).__init__(datablock, byteswap=byteswap)
        self.read(datablock[self.hdr_sz:])

    def read(self, datablock):
        """
        Reads the data portion of this datablock.  Data formats are defined
        after the header is read to accomidate sizes defined in the header.
        All values are converted to whole units.
        """
        Ns = self.header['#SamplesInDatagram']
        Ne = self.header['#StavesPerSample']
        read_fmt = str(Ne) + 'b'
        used_fmt = str(Ne) + 'f'
        read_dtype = np.dtype([('Sample#', 'H'), ('TvgGain', "h"),
                               ('StaveBackscatter', read_fmt)])
        self._read_dtype = read_dtype
        read_sz = read_dtype.itemsize
        used_dtype = np.dtype([('Sample#', 'H'), ('TvgGain', "f"),
                               ('StaveBackscatter', read_fmt)])
        self.data = np.frombuffer(datablock[:Ns * read_sz],
                                  dtype=read_dtype)
        self.data = self.data.astype(used_dtype)
        self.data['TvgGain'] *= 0.01
        self.data['StaveBackscatter'] *= 0.5

    def get_datablock(self, data=None):
        part1 = super(Data109, self).get_datablock()
        tmp_data = self.data.copy()
        tmp_data['TvgGain'] /= 0.01
        tmp_data['StaveBackscatter'] /= 0.5
        tmp_data = self.data.astype(self._read_dtype)
        # part2 = np.getbuffer(tmp_data)
        part2 = tmp_data.tobytes()
        return part1 + part2


class Data110_grp(BaseData):
    hdr_dtype = np.dtype([('GroupStart', 'S4'), ('GroupID', 'H'),
                          ('ByteCount', 'H'), ('Time1', 'd'), ('Time2', 'd'),
                          ('DistanceTag', 'd'), ('TimeTypes', 'B'), ('DistanceType', 'B'),
                          ('Latitude', 'd'), ('Longitude', 'd'), ('Altitude', 'd'),
                          ('AlongTrackVelocity', 'f'), ('AcrossTrackVelocity', 'f'),
                          ('DownVelocity', 'f'), ('Roll', 'd'), ('Pitch', 'd'),
                          ('Heading', 'd'), ('WanderAngle', 'd'), ('Heave', 'f'),
                          ('RollRate', 'f'), ('PitchRate', 'f'), ('YawRate', 'f'),
                          ('LongitudinalAcceleration', 'f'), ('TransverseAcceleration', 'f'),
                          ('DownAcceleration', 'f'), ('Pad', 'H'), ('CheckSum', 'H'),
                          ('MessageEnd', 'S2')])

    def __init__(self, datablock, POSIXtime, byteswap=False, read_limit=None):
        super(Data110_grp, self).__init__(datablock, byteswap=byteswap,
                                          read_limit=read_limit)  # read as many records as passed in
        self.time = POSIXtime
        packettime = dtm.datetime.utcfromtimestamp(POSIXtime)
        # subtract 1 because the first day of the year does not start with zero
        ordinal = packettime.toordinal()
        dow = packettime.weekday() + 1.
        if dow == 7:
            # shift sunday to be start of week.
            dow = 0
        # 1970-1-1 is julian day 719163
        POSIXdays = ordinal - 719163. - dow
        self.weektime = POSIXdays * 24. * 3600.


class Data110_aaq(BaseData):
    hdr_dtype = np.dtype([('Header1', 'B'), ('Header2', 'B'),
                          ('Seconds', 'i'), ('FracSeconds', 'f'), ('Latitude', 'f'),
                          ('Longitude', 'f'), ('Height', 'f'), ('Heave', 'f'),
                          ('NorthVelocity', 'f'), ('EastVelocity', 'f'),
                          ('DownVelocity', 'f'), ('Roll', 'f'), ('Pitch', 'f'),
                          ('Heading', 'f'), ('RollRate', 'f'), ('PitchRate', 'f'),
                          ('YawRate', 'f'), ('StatusWord', 'H'), ('CheckSum', 'H')])
    raw_dtype = np.dtype([('Header1', 'B'), ('Header2', 'B'),
                          ('Seconds', '>i'), ('FracSeconds', '>H'), ('Latitude', '>i'),
                          ('Longitude', '>i'), ('Height', '>i'), ('Heave', '>h'),
                          ('NorthVelocity', '>h'), ('EastVelocity', '>h'),
                          ('DownVelocity', '>h'), ('Roll', '>h'), ('Pitch', '>h'),
                          ('Heading', '>H'), ('RollRate', '>h'), ('PitchRate', '>h'),
                          ('YawRate', '>h'), ('StatusWord', '>H'), ('CheckSum', '>H')])
    conversions = {'FracSeconds': 0.0001, 'Latitude': 90. / 2 ** 30, 'Longitude': 90. / 2 ** 30,
                   'Height': 0.01, 'Heave': 0.01, 'NorthVelocity': 0.01, 'EastVelocity': 0.01,
                   'DownVelocity': 0.01, 'Roll': 90. / 2 ** 14, 'Pitch': 90. / 2 ** 14,
                   'Heading': 90. / 2 ** 14, 'RollRate': 90. / 2 ** 14,
                   'PitchRate': 90. / 2 ** 14, 'YawRate': 90. / 2 ** 14}

    def __init__(self, datablock, POSIXtime, byteswap=False, read_limit=None):
        super(Data110_aaq, self).__init__(datablock, byteswap=byteswap,
                                          read_limit=read_limit)  # read as many records as passed in


class Data110_q42(BaseData):
    hdr_dtype = np.dtype([('Header', 'S1'),
                          ('Seconds', 'i'), ('FracSeconds', 'f'), ('Latitude', 'f'),
                          ('Longitude', 'f'), ('Height', 'f'), ('Heave', 'f'),
                          ('NorthVelocity', 'f'), ('EastVelocity', 'f'),
                          ('DownVelocity', 'f'), ('Roll', 'f'), ('Pitch', 'f'),
                          ('Heading', 'f'), ('RollRate', 'f'), ('PitchRate', 'f'),
                          ('YawRate', 'f'), ('StatusWord', 'H'), ('CheckSum', 'H')])
    raw_dtype = np.dtype([('Header', 'S1'),
                          ('Seconds', '>i'), ('FracSeconds', '>B'), ('Latitude', '>i'),
                          ('Longitude', '>i'), ('Height', '>i'), ('Heave', '>h'),
                          ('NorthVelocity', '>h'), ('EastVelocity', '>h'),
                          ('DownVelocity', '>h'), ('Roll', '>h'), ('Pitch', '>h'),
                          ('Heading', '>H'), ('RollRate', '>h'), ('PitchRate', '>h'),
                          ('YawRate', '>h'), ('StatusWord', '>H'), ('CheckSum', '>H')])
    conversions = {'FracSeconds': 0.01, 'Latitude': 90. / 2 ** 30, 'Longitude': 90. / 2 ** 30,
                   'Height': 0.01, 'Heave': 0.01, 'NorthVelocity': 0.01, 'EastVelocity': 0.01,
                   'DownVelocity': 0.01, 'Roll': 90. / 2 ** 14, 'Pitch': 90. / 2 ** 14,
                   'Heading': 90. / 2 ** 14, 'RollRate': 90. / 2 ** 14,
                   'PitchRate': 90. / 2 ** 14, 'YawRate': 90. / 2 ** 14}

    def __init__(self, datablock, POSIXtime, byteswap=False, read_limit=None):
        super(Data110_q42, self).__init__(datablock, byteswap=byteswap,
                                          read_limit=read_limit)  # read as many records as passed in


class Data110_att(BaseData):
    hdr_dtype = np.dtype([('Time', 'd'), ('Roll', 'f'), ('Pitch', 'f'), ('Heave', 'f'),
                          ('Heading', 'f'), ('NumBytesInput', 'B')])
    raw_dtype = np.dtype([('Time', 'H'), ('Roll', 'h'), ('Pitch', 'h'), ('Heave', 'h'),
                          ('Heading', 'H'), ('NumBytesInput', 'B')])

    def __init__(self, datablock, POSIXtime, byteswap=False, read_limit=None):
        """Catches the binary datablock and decodes the first section and calls
        the decoder for the rest of the record."""
        if read_limit is None:
            raise Exception("read_limit can not be None for Data110_att")
        # Data110 can have an odd number of bytes for the datablock.  Kongsberg adds a \x00 byte to the datablock if
        #   datagram is an odd number of bytes.
        if len(datablock) % 2 and datablock.endswith(b'\x00'):
            datablock = datablock[:len(datablock) - 1]
        raw_data_size = int(len(datablock) / read_limit - self.hdr_sz)
        raw_data_dtype = np.dtype([('Raw', 'S' + str(raw_data_size))])
        combined_dtype = np.dtype([('Proc', Data110_att.raw_dtype), ('Raw', raw_data_dtype)])
        temp = np.frombuffer(datablock[:read_limit * combined_dtype.itemsize], dtype=combined_dtype)
        self.header = temp['Proc'].astype(Data110_att.hdr_dtype)
        self.time = POSIXtime
        self._parse_raw(temp['Raw'], raw_data_size, read_limit)
        # self.data = np.zeros(self.numrecords, dtype = att_file_dtype)
        # datap = 0
        # for i in range(self.numrecords):
        # temp = np.frombuffer(datablock[datap:att_sz+datap],
        # dtype = att_file_dtype)
        # datap += att_sz + temp['NumBytesInput'][0]
        # self.data[i] = temp[['Time', 'Roll', 'Pitch', 'Heave', 'Heading']].astype(Data110.att_dtype)
        self.header['Time'] = self.header['Time'] * 0.001 + self.time
        self.header['Roll'] *= 0.01
        self.header['Pitch'] *= 0.01
        self.header['Heave'] *= 0.01
        self.header['Heading'] *= 0.01

    def _parse_raw(self, raw_arrays, raw_data_size, read_limit):
        """
        Parses the raw data that arrived in SIS and puts it in source_data.  If
        the data type is not known source_data is None.
        """
        first_record = raw_arrays[0][0]
        if first_record[0:4] == b'$GRP':
            datablock = b""
            for r in raw_arrays:
                datablock += r[0][:Data110_grp.hdr_sz]
            self.raw_data = Data110_grp(datablock, self.time, read_limit=read_limit)
            self.source_data = self.raw_data.header
            self.weektime = self.raw_data.weektime
            self.source = 'GRP102'
        elif first_record[:2] == b'\xaaQ':
            datablock = b""
            for r in raw_arrays:
                datablock += r[0][:Data110_aaq.hdr_sz]
            self.raw_data = Data110_aaq(datablock, self.time, read_limit=read_limit)
            self.source_data = self.raw_data.header
            self.source = 'binary23'
        elif first_record[0] == b'q' and raw_data_size == 42:
            datablock = b""
            for r in raw_arrays:
                datablock += r[0][:Data110_q42.hdr_sz]
            self.raw_data = Data110_q42(datablock, self.time, read_limit=read_limit)
            self.source_data = self.raw_data.header
            self.source = 'binary11'
        else:
            # self.source_data = np.getbuffer(np.ascontiguousarray(raw_arrays))
            self.source_data = np.ascontiguousarray(raw_arrays).tobytes()
            self.source = 'Unknown'

    def get_datablock(self, data=None):
        tmp_header = self.header.copy()
        tmp_header['Time'] = (tmp_header['Time'] + self.time) / 0.001
        tmp_header['Roll'] /= 0.01
        tmp_header['Pitch'] /= 0.01
        tmp_header['Heave'] /= 0.01
        tmp_header['Heading'] /= 0.01
        part1 = super(Data110_att, self).get_datablock(tmp_header)
        raise Exception("Need to put the Proc and Raw packets back together properly.")

        part2 = self.att_data.get_datablock()
        part3 = self.source_data.get_datablock()
        return part1 + part2 + str(part3)


class Data110(BaseData):
    """
    The Network Attitiude Datagram, 6Eh / 110d / 'n'.  Data is found in the header
    and in the 'data' array.  All values are in degrees, and meters.  The raw
    data is parsed and placed in source_data.
    """

    hdr_dtype = np.dtype([('Counter', 'H'), ('Serial#', 'H'), ('NumEntries', 'H'),
                          ('Sensor', 'B'), ('Spare', 'B')])

    def __init__(self, datablock, POSIXtime, byteswap=False):
        """Catches the binary datablock and decodes the first section and calls
        the decoder for the rest of the record."""
        super(Data110, self).__init__(datablock, byteswap=byteswap)
        self.time = POSIXtime
        self.read(datablock[self.hdr_sz:])

    def read(self, datablock):
        """Reads the data section of the record.  Time is POSIX time,
        angles are in degrees, distances in meters."""
        self.numrecords = self.header[2]
        self.att_data = Data110_att(datablock, self.time, read_limit=self.numrecords)
        # maintain backward compatibility
        self.data = self.att_data.header
        self.source_data = self.att_data.source_data
        self.source = self.att_data.source

    def get_datablock(self, data=None):
        part1 = super(Data110, self).get_datablock()
        part2 = self.att_data.get_datablock()
        part3 = self.source_data.get_datablock()
        return part1 + part2 + str(part3)


class mappack:
    """
    Container for the file packet map.
    """

    def __init__(self):
        """Constructor creates a packmap dictionary"""
        self.packdir = {}
        self.sizedir = {}
        self.numwc = None
        self.dtypes = {
            68: 'Old Depth',
            88: 'New Depth',
            102: 'Old Range/Angle',
            78: 'New Rangle/Angle',
            83: 'Old Seabed Imagry',
            89: 'New Seabead Imagry',
            107: 'Watercolumn',
            79: 'Quality Factor',
            65: 'Serial Attitude',
            110: 'Network Attitude',
            67: 'Clock',
            72: 'Heading',
            80: 'Position',
            71: 'Surface Sound Speed',
            85: 'Sound Speed Profile',
            73: 'Start Parameters',
            105: 'Stop Parameters',
            112: 'Remote Parameters',
            82: 'Runtime Parameters',
            104: 'Height',
            48: 'PU ID Output',
            49: 'PU Status',
            66: 'PU BIST Results',
            51: 'Extra Parameters'
        }

    def add(self, type, location=0, time=0, size=0, pingcounter=None):
        """Adds the location (byte in file) to the tuple for the value type"""
        if type in self.packdir:
            if pingcounter is None:
                self.packdir[type].append([location, time, size])
            else:
                self.packdir[type].append([location, time, size, pingcounter])
            self.sizedir[type] += size
        else:
            if pingcounter is None:
                self.packdir[type] = []
                self.packdir[type].append([location, time, size])
            else:
                self.packdir[type] = []
                self.packdir[type].append([location, time, size, pingcounter])
            self.sizedir[type] = size

    def finalize(self):
        for key in list(self.packdir.keys()):
            temp = np.asarray(self.packdir[key])
            tempindx = temp[:, 1].argsort()
            self.packdir[key] = temp[tempindx, :]

    def printmap(self):
        keys = []
        totalsize = 0
        for i, v in self.packdir.items():
            keys.append((int(i), len(v)))
            totalsize += self.sizedir[i]
        keys.sort()
        for key in keys:
            dtype = self.gettype(key[0])
            percent = 10000 * self.sizedir[str(key[0])] / totalsize
            print(dtype + ' ' + str(key[0]) + ' (' + hex(int(key[0])) + ') has ' + str(key[1]
                                                                                       ) + ' packets and is ' + str(
                0.01 * percent) + '% of file.')

    def getnum(self, recordtype):
        """
        Returns the number of records of the provided record type.
        """
        return len(self.packdir[str(recordtype)])

    def plotmap(self):
        """
        Plots to location of each of the packets in the file.
        """
        keys = sorted(self.packdir.keys())
        plt.figure()
        for key in keys:
            plt.plot(self.packdir[key][:, 0])
        plt.xlabel('Packet Number')
        plt.ylabel('Location in file')
        plt.legend(keys, loc='lower right')
        plt.grid()

    def save(self, outfilename):
        outfile = open(outfilename, 'wb')
        pickle.dump(self.__dict__, outfile)
        outfile.close()

    def gettype(self, dtype):
        if int(dtype) in self.dtypes:
            out = self.dtypes[int(dtype)]
        else:
            out = ''
        return out

    def load(self, infilename):
        infile = open(infilename, 'rb')
        self.__dict__ = pickle.load(infile)
        infile.close()


def _run_sequential_read(fildata):
    """
    Function for managing par.sequential_read_records.  Takes in .all files, outputs dict of records

    Returns
    -------
    pd.sequential_read_records: dict
        Dictionary where keys are the datagram type numbers and values are dicts of columns/rows from datagram

    """
    fil, offset, endpt = fildata
    pnds = AllRead(fil, start_ptr=offset, end_ptr=endpt)
    return pnds.sequential_read_records()


def _sequential_to_xarray(rec):
    """
    After running sequential read, this method will take in the dict of datagrams and return an xarray.

    Returns
    -------
    final: Xarray.Dataset
        - coordinates include time, sectors, beam, profile_depth
        - variables include roll, pitch, heave, heading, soundspeed, ntx, nrx, nvalid, samplerate, transmitsector#,
           tiltangle, signallength, delay, frequency, waveformid, beampointingangle, transmitsectorid, detectioninfo,
           qualityfactor, traveltime, speed, course, soundspeedprofile_XXXXX, latitude, longitude, alongtrackvelocity,
           altitude
    """
    if '78' not in rec:
        print('No ping record found for chunk file')
        return

    recs_to_merge = {}
    tims = np.unique(rec['78']['time'])

    for r in rec:
        if r not in ['73', '85']:
            # each Kongsberg datagram is a Xarray Dataset, each record in the datagram is a Xarray DataArray
            recs_to_merge[r] = xr.Dataset()
            for ky in rec[r]:
                if ky != 'time':
                    if r == '78':
                        # For some reason I haven't figured out yet, you end up with a duplicate ping at the end,
                        #   intermittently when running this with chunked files...found it out because the number of
                        #   unique times didn't line up with record length.  See the np.all checks below
                        combined_sectors = np.array(np.split(rec['78'][ky], len(rec['78'][ky]) / 3))

                        # these records are by time/sector/beam.  Have to combine recs to build correct array shape
                        if ky in ['beampointingangle', 'transmitsectorid', 'detectioninfo', 'qualityfactor',
                                  'traveltime']:
                            if np.all(combined_sectors[-2] == combined_sectors[-1]) and combined_sectors.shape[0] != len(tims):
                                combined_sectors = combined_sectors[0:combined_sectors.shape[0] - 1, :, :]
                            beam_idx = [i for i in range(combined_sectors.shape[2])]
                            sectors = [i for i in range(combined_sectors.shape[1])]
                            recs_to_merge[r][ky] = xr.DataArray(combined_sectors,
                                                                coords=[tims, sectors, beam_idx],
                                                                dims=['time', 'sectors', 'beam'])
                        #  everything else isn't by beam, proceed normally
                        else:
                            if np.all(combined_sectors[-2] == combined_sectors[-1]) and combined_sectors.shape[0] != len(tims):
                                combined_sectors = combined_sectors[0:combined_sectors.shape[0] - 1, :]
                            sectors = [i for i in range(combined_sectors.shape[1])]
                            recs_to_merge[r][ky] = xr.DataArray(combined_sectors,
                                                                coords=[tims, sectors],
                                                                dims=['time', 'sectors'])
                    else:
                        recs_to_merge[r][ky] = xr.DataArray(rec[r][ky], coords=[rec[r]['time']], dims=['time'])

    # take range/angle rec and merge on to that index, gets attitude/positioning aligned with the ping
    for r in recs_to_merge:
        if r not in ['73', '78', '85']:
            if r == '110':  # Data110 has duplicate values, should probably check on why this is
                _, index = np.unique(recs_to_merge[r]['time'], return_index=True)
                recs_to_merge[r] = recs_to_merge[r].isel(time=index)
            recs_to_merge[r] = recs_to_merge[r].reindex_like(recs_to_merge['78'], method='nearest')

    # Merge the dataset with inner join: intersection of time index
    final = xr.merge([recs_to_merge[r] for r in recs_to_merge if r not in ['73']], join='inner')

    # add on attribute for installation parameters, basically the same way as you do for the ss profile, except it
    #   has no coordinate to index by.  Also, use json.dumps to avoid the issues with serializing lists/dicts with
    #   to_netcdf
    if '73' in rec:
        for t in rec['73']['time']:
            idx = np.where(rec['73']['time'] == t)
            final.attrs['settings_{}'.format(int(t))] = json.dumps(rec['73']['settings'][idx][0])

    if '85' in rec:
        for t in rec['85']['time']:
            idx = np.where(rec['85']['time'] == t)
            profile = np.dstack([rec['85']['depth'][idx][0], rec['85']['soundspeed'][idx][0]])[0]
            final.attrs['profile_{}'.format(int(t))] = json.dumps(profile.tolist())

    return final


def _xarr_to_netcdf(pth, xarr):
    """
    Runs xarray.to_netcdf on given file

    Returns
    -------
    None

    """
    xarr.to_netcdf(path=pth, mode='w')


class BatchRead:
    """
    BatchRead - Kongsberg .all data converter using dask infrastructure
    pass in filfolder full of .all files, call read(), and gain access to xarray Dataset object

    BatchRead is stored internally using the following conventions:
    - X = + Forward, Y = + Starboard, Z = + Down
    - roll = + Port Up, pitch = + Bow Up, gyro = + Clockwise

    self.rawdat will be dask delayed object, if you need to pull locally, use the self.rawdat.compute() method

    In[3]: import par3 as par

    Backend Qt5Agg is interactive backend. Turning interactive mode on.

    In[4]: cnverted = par.BatchRead(r'C:\Collab\dasktest\data_dir')

    In[5]: cnverted.read()

    Started local cluster client...
    Running Kongsberg .all converter...
    C:\Collab\dasktest\data_dir\0001_20170822_144548_S5401_X.all: Using 6 chunks of size 44296946
    C:\Collab\dasktest\data_dir\0003_20170822_150341_S5401_X.all: Using 8 chunks of size 40800594
    C:\Collab\dasktest\data_dir\0005_20170822_152146_S5401_X.all: Using 7 chunks of size 41886286
    C:\Collab\dasktest\data_dir\0007_20170822_153922_S5401_X.all: Using 6 chunks of size 43280232
    C:\Collab\dasktest\data_dir\0009_20170822_155626_S5401_X.all: Using 7 chunks of size 42632467
    C:\Collab\dasktest\data_dir\0010_20170822_160627_S5401_X.all: Using 5 chunks of size 41804621
    C:\Collab\dasktest\data_dir\0011_20170822_161430_S5401_X.all: Using 7 chunks of size 43407640
    C:\Collab\dasktest\data_dir\0012_20170822_162915_S5401_X.all: Using 5 chunks of size 43478335
    Distributed conversion complete: 64.8518365s

    In[6]: cnverted.rawdat

    Out[6]:
    <xarray.Dataset>
    Dimensions:             (beam: 284, sectors: 3, time: 92186)
    Coordinates:
      * beam                (beam) int64 0 1 2 3 4 5 6 ... 278 279 280 281 282 283
      * sectors             (sectors) int32 0 1 2
      * time                (time) float64 1.503e+09 1.503e+09 ... 1.503e+09
    Data variables:
        roll                (time) float32 dask.array<chunksize=(1927,), meta=np.ndarray>
        pitch               (time) float32 dask.array<chunksize=(1927,), meta=np.ndarray>
        heave               (time) float32 dask.array<chunksize=(1927,), meta=np.ndarray>
        heading             (time) float32 dask.array<chunksize=(1927,), meta=np.ndarray>
        soundspeed          (time, sectors) float32 dask.array<chunksize=(1927, 3), meta=np.ndarray>
        ntx                 (time, sectors) uint16 dask.array<chunksize=(1927, 3), meta=np.ndarray>
        nrx                 (time, sectors) uint16 dask.array<chunksize=(1927, 3), meta=np.ndarray>
        nvalid              (time, sectors) uint16 dask.array<chunksize=(1927, 3), meta=np.ndarray>
        samplerate          (time, sectors) float32 dask.array<chunksize=(1927, 3), meta=np.ndarray>
        transmitsector#     (time, sectors) uint8 dask.array<chunksize=(1927, 3), meta=np.ndarray>
        tiltangle           (time, sectors) float32 dask.array<chunksize=(1927, 3), meta=np.ndarray>
        signallength        (time, sectors) float32 dask.array<chunksize=(1927, 3), meta=np.ndarray>
        delay               (time, sectors) float32 dask.array<chunksize=(1927, 3), meta=np.ndarray>
        frequency           (time, sectors) float32 dask.array<chunksize=(1927, 3), meta=np.ndarray>
        waveformid          (time, sectors) uint8 dask.array<chunksize=(1927, 3), meta=np.ndarray>
        beampointingangle   (time, sectors, beam) float64 dask.array<chunksize=(1927, 3, 284), meta=np.ndarray>
        transmitsectorid    (time, sectors, beam) float64 dask.array<chunksize=(1927, 3, 284), meta=np.ndarray>
        detectioninfo       (time, sectors, beam) float64 dask.array<chunksize=(1927, 3, 284), meta=np.ndarray>
        qualityfactor       (time, sectors, beam) float64 dask.array<chunksize=(1927, 3, 284), meta=np.ndarray>
        traveltime          (time, sectors, beam) float64 dask.array<chunksize=(1927, 3, 284), meta=np.ndarray>
        speed               (time) float32 dask.array<chunksize=(1927,), meta=np.ndarray>
        course              (time) float32 dask.array<chunksize=(1927,), meta=np.ndarray>
        latitude            (time) float64 dask.array<chunksize=(1927,), meta=np.ndarray>
        longitude           (time) float64 dask.array<chunksize=(1927,), meta=np.ndarray>
        alongtrackvelocity  (time) float32 dask.array<chunksize=(1927,), meta=np.ndarray>
        altitude            (time) float64 dask.array<chunksize=(1927,), meta=np.ndarray>
    Attributes:
        settings_1503413148:  {"waterline_vertical_location": "-1.010", "system_m...
        profile_1503411780:   [[0.0, 1517.0999755859375], [0.15, 1517.09997558593...

    """
    def __new__(cls, filfolder, address=None, minchunksize=40000000, max_chunks=20):
        if not batch_read_enabled:
            print('Dask and Xarray are required dependencies to run BatchRead.  Please ensure you have these modules ' +
                  'first.')
            return None
        else:
            return super(BatchRead, cls).__new__(cls)

    def __init__(self, filfolder, address=None, minchunksize=40000000, max_chunks=20):
        self.filfolder = filfolder
        self.convert_minchunksize = minchunksize
        self.convert_maxchunks = max_chunks
        self.address = address
        self.rawdat = None
        self.localrawdat = None
        self.readsuccess = False
        self.client = self.dask_find_or_start_client(address=self.address)

        # misc
        self.extents = None

        # install parameters
        self.sonartype = None
        self.xyzrph = None

        # fqpr recs
        self.nav_motion = None
        self.fqprdat = None

        self.ky_data73_sonar_translator = {'em122': [None, 'tx', 'rx', None], 'em302': [None, 'tx', 'rx', None],
                                           'em710': [None, 'tx', 'rx', None], 'em2040': [None, 'tx', 'rx', None],
                                           'em2040_dual_rx': [None, 'tx', 'rx_port', 'rx_stbd'],
                                           'em2040_dual_tx': ['tx_port', 'tx_stbd', 'rx_port', 'rx_stbd'],
                         # 'em2040c': [None, 'sonar_head1', 'sonar_head2', None],  not sure how to identify this yet
                                           'em3002': [None, 'sonar_head1', 'sonar_head2', None],
                                           'em2040p': [None, 'sonar_head1', None, None],
                                           'me70bo': ['transducer', None, None, None]}

    def _closest_key_value(self, sortdict, key):
        """
        Return closest key in `sorted_dict` to given `key`.
        """
        # convert between float/int/string as necessary for given key
        # - return it in the type given by key, i.e. convert it back to the type the user entered

        try:
            ktyp = str
            keys = list(islice(sortdict.irange(minimum=key), 1))
        except TypeError:
            if type(key) in [float, int]:
                key = str(key)
                ktyp = str
            else:
                key = float(key)
                ktyp = float
            keys = list(islice(sortdict.irange(minimum=key), 1))
        # build out list of the closest highest/lowest from the given key value
        keys.extend(list(islice(sortdict.irange(maximum=key, reverse=True), 1)))
        floatkeys = [float(k) for k in keys]

        if floatkeys:
            should_be_key = ktyp(min(floatkeys, key=lambda k: abs(float(key) - k)))
            if should_be_key not in sortdict and ktyp is str:
                if should_be_key.rstrip('.0') in sortdict:
                    should_be_key = should_be_key.rstrip('.0')
                    return should_be_key
            elif should_be_key in sortdict:
                return should_be_key
            else:
                print('Unable to determine key from given timestamp: {} with given {}'.format(list(sortdict.keys()),
                                                                                              key))
                return None
        return None

    def read(self, remove_localrawdat=True):
        """
        Run the batch_read method on all available lines, can clear out existing local data just in case
        """

        fils = self.batch_read()
        self.read_from_netcdf_fils(fils)

        if self.rawdat is not None:
            self.readsuccess = True
            self.build_offsets()
            if remove_localrawdat:
                # If you read in new data, you probably want to remove the local dataframe copies if they exist just to
                #   avoid confusion
                self.localrawdat = None

    def read_from_netcdf_fils(self, file_list):
        """
        Read from the generated netCDF files constructed with read()
        """
        # I'm having to close the LocalCluster before running open_mfdataset to avoid these worker Errors.  Need to
        #   understand this a bit more.
        self.client.close()
        self.rawdat = xr.open_mfdataset(file_list, combine='nested', concat_dim='time', engine='netcdf4')

    def get_local_data(self):
        """
        Compute the xarray to generate numpy arrays from xarray/dask delayed arrays
        """
        if self.rawdat is None:
            print('Generate raw data first with read() method.')
        else:
            self.localrawdat = self.rawdat.compute()

    def dask_find_or_start_client(self, address=None, silent=False):
        """
        Either start or return Dask client in local/networked cluster mode

        Returns
        -------
        client: dask.distributed.client.Client instance
            Client instance representing Local Cluster/Networked Cluster operations

        """
        try:
            if address is None:
                client = get_client()
                if not silent:
                    print('Using existing local cluster client...')
            else:
                client = get_client(address=address)
                if not silent:
                    print('Using existing client on address {}...'.format(address))
        except ValueError:
            if address is None:
                client = Client()
                if not silent:
                    print('Started local cluster client...')
            else:
                client = Client(address=address)
                if not silent:
                    print('Started client on address {}...'.format(address))
        return client

    def batch_read(self):
        """
        General converter for .all files leveraging xarray and dask.distributed
        Only parses datagrams as determined by par.recs_categories

        Takes in a wrkdir of .all files, outputs a list of netCDF files written from xarray, ready to be read using
           xarray.open_mfdataset

        Returns
        -------
        f_pths: list of netCDF files

        """
        print('Running Kongsberg .all converter...')
        starttime = perf_counter()

        if self.client is None:
            self.client = self.dask_find_or_start_client()

        if self.client is not None:
            fils = glob(os.path.join(self.filfolder, '*.all'))
            webbrowser.open_new('http://localhost:8787/status')

            chnks = []
            for f in fils:
                chnks.append(return_chunked_fil(f, 0, determine_good_chunksize(f, self.convert_minchunksize,
                                                                               self.convert_maxchunks)))
            chnks_flat = [c for subc in chnks for c in subc]
            # chnks_flat is now a list of lists representing chunks of each file

            recfutures = self.client.map(_run_sequential_read, chnks_flat)
            # recfutures is a list of futures representing dicts from sequential read

            xarrfutures = self.client.map(_sequential_to_xarray, recfutures)
            # xarrfutures is a list of futures representing xarray structures for each file chunk

            if not os.path.exists(os.path.join(self.filfolder, 'converted')):
                os.makedirs(os.path.join(self.filfolder, 'converted'))
            f_pths = [os.path.join(self.filfolder, 'converted', 'conversion_all_{}.nc'.format(idx)) for idx in
                      range(len(xarrfutures))]
            writefuts = self.client.map(_xarr_to_netcdf, f_pths, xarrfutures)
            wait(writefuts)

            # client.submit(xr.combine_by_coords, [xarrfutures])

            endtime = perf_counter()
            print('Distributed conversion complete: {}s\n'.format(endtime - starttime))

            return f_pths
        return None

    def build_offsets(self):
        """
        Form time series pandas dataframe for unique entries in installation parameters across all lines
        """
        self.xyzrph = {}

        settdict = {}
        setts = [x for x in self.rawdat.attrs if x[0:8] == 'settings']
        for sett in setts:
            settdict[sett.split('_')[1]] = json.loads(self.rawdat.attrs[sett])

        print('Found {} total Installation Parameters entr(y)s'.format(len(settdict)))
        if len(settdict) > 0:
            snrmodels = np.unique([settdict[x]['sonar_model_number'] for x in settdict])
            if len(snrmodels) > 1:
                print('ERROR: Found multiple sonars types in data provided')
                return
            if snrmodels[0] not in self.ky_data73_sonar_translator:
                print('ERROR: Sonar model not understood "{}"'.format(snrmodels[0]))
                return

            # translate over the offsets/angles for the transducers following the sonar_translator scheme
            self.sonartype = snrmodels[0]
            for tme in settdict:
                self.xyzrph[tme] = {}
                for val in [v for v in self.ky_data73_sonar_translator[snrmodels[0]] if v is not None]:  # tx, rx, etc.
                    ky = self.ky_data73_sonar_translator[snrmodels[0]].index(val)  # 0, 1, 2, etc
                    self.xyzrph[tme][val + '_x'] = settdict[tme]['transducer_{}_along_location'.format(ky)]
                    self.xyzrph[tme][val + '_y'] = settdict[tme]['transducer_{}_athwart_location'.format(ky)]
                    self.xyzrph[tme][val + '_z'] = settdict[tme]['transducer_{}_vertical_location'.format(ky)]
                    self.xyzrph[tme][val + '_r'] = settdict[tme]['transducer_{}_roll_angle'.format(ky)]
                    self.xyzrph[tme][val + '_p'] = settdict[tme]['transducer_{}_pitch_angle'.format(ky)]
                    self.xyzrph[tme][val + '_h'] = settdict[tme]['transducer_{}_heading_angle'.format(ky)]

                # translate over the positioning sensor stuff using the installation parameters active identifiers
                pos_ident = settdict[tme]['active_position_system_number']   # 'position_1'
                for suffix in [['_vertical_location', '_z'], ['_along_location', '_x'],
                               ['_athwart_location', '_y'], ['_time_delay', '_latency']]:
                    qry = pos_ident + suffix[0]
                    self.xyzrph[tme]['POSMV' + suffix[1]] = settdict[tme][qry]

                # do the same over motion sensor (which is still the POSMV), make assumption that its one of the motion
                #   entries
                pos_motion_ident = settdict[tme]['active_heading_sensor'].split('_')
                pos_motion_ident = pos_motion_ident[0] + '_sensor_' + pos_motion_ident[1]  # 'motion_1_com2'

                for suffix in [['_vertical_location', '_motionz'], ['_along_location', '_motionx'],
                               ['_athwart_location', '_motiony'], ['_time_delay', '_motionlatency'],
                               ['_roll_angle', '_r'], ['_pitch_angle', '_p'], ['_heading_angle', '_h']]:
                    qry = pos_motion_ident + suffix[0]
                    self.xyzrph[tme]['POSMV' + suffix[1]] = settdict[tme][qry]

                # generate dict of ordereddicts for fast searching
                newdict = {}
                for ky in self.xyzrph:
                    for stmp in self.xyzrph[ky].keys():
                        if stmp not in newdict:
                            newdict[stmp] = SortedDict()
                        newdict[stmp][ky] = self.xyzrph[ky][stmp]
                self.xyzrph = SortedDict(newdict)
                print('Constructed offsets successfully')

    def return_tx_rph(self, time_idx):
        """
        Using the constructed xyzrph attribute (see build_offsets) and a given timestamp, return the roll/pitch/heading
        transmitter offset nearest in time to the timestamp

        Parameters
        ----------
        time_idx = timestamp (accepts int/str/float)

        Returns
        -------
        txr, txp, txh = mounting angle offsets for roll/pitch/heading respectively

        """
        if self.xyzrph is None:
            self.build_offsets()
        if self.sonartype == 'em2040':
            corr_timestmp = self._closest_key_value(self.xyzrph['tx_r'], time_idx)
            txr = float(self.xyzrph['tx_r'][corr_timestmp])
            txp = float(self.xyzrph['tx_p'][corr_timestmp])
            txh = float(self.xyzrph['tx_h'][corr_timestmp])
            return {corr_timestmp: {'tx_roll': txr, 'tx_pitch': txp, 'tx_heading': txh}}
        else:
            raise NotImplementedError('Only EM2040 supported at this time')

    def return_rx_rph(self, time_idx):
        """
        Using the constructed xyzrph attribute (see build_offsets) and a given timestamp, return the roll/pitch/heading
        receiver offset nearest in time to the timestamp

        Parameters
        ----------
        time_idx = timestamp (accepts int/str/float)

        Returns
        -------
        rxr, rxp, rxh = mounting angle offsets for roll/pitch/heading respectively

        """
        if self.xyzrph is None:
            self.build_offsets()
        if self.sonartype == 'em2040':
            corr_timestmp = self._closest_key_value(self.xyzrph['rx_r'], time_idx)
            rxr = float(self.xyzrph['rx_r'][corr_timestmp])
            rxp = float(self.xyzrph['rx_p'][corr_timestmp])
            rxh = float(self.xyzrph['rx_h'][corr_timestmp])
            return {corr_timestmp: {'rx_roll': rxr, 'rx_pitch': rxp, 'rx_heading': rxh}}
        else:
            raise NotImplementedError('Only EM2040 supported at this time')

    def get_minmax_extents(self):
        """
        Build dataset geographic extents
        """
        maxlat = self.rawdat.latitude.max().compute()
        maxlon = self.rawdat.longitude.max().compute()
        minlat = self.rawdat.latitude.min().compute()
        minlon = self.rawdat.longitude.min().compute()
        print('Max Lat/Lon: {}/{}'.format(maxlat, maxlon))
        print('Min Lat/Lon: {}/{}'.format(minlat, minlon))
        self.extents = [maxlat, maxlon, minlat, minlon]

    def show_description(self):
        """
        Display xarray Dataset description
        """
        print(self.rawdat.info)

    def generate_plots(self):
        """
        Generate some example plots showing the abilities of the xarray plotting engine
        - plot detection info for beams/sectors/times
        - plot roll/pitch/heave on 3 row plot
        """
        self.rawdat.detectioninfo.plot(x='beam', y='time', col='sectors', col_wrap=3)

        fig, axes = plt.subplots(nrows=3)
        self.rawdat['roll'].plot(ax=axes[0])
        self.rawdat['pitch'].plot(ax=axes[1])
        self.rawdat['heading'].plot(ax=axes[2])


def plot_all_nav(directory='.'):
    """
    Plots the parts of the navarray from all files in the directory.
    """
    fig = plt.figure()
    ax1 = fig.add_subplot(221)
    ax2 = fig.add_subplot(222)
    ax3 = fig.add_subplot(614, sharex=ax2)
    ax4 = fig.add_subplot(615, sharex=ax2)
    ax5 = fig.add_subplot(616, sharex=ax2)
    flist = sorted(glob(directory + '/*.all'))
    clist = ['b', 'r', 'g', 'k', 'y', 'c']
    n = 0
    for f in flist:
        a = AllRead(f)
        if os.path.exists(f + '.nav'):
            a.load_navarray()
        else:
            a._build_navarray()
            a.save_navarray()
        ax1.plot(a.navarray['80'][:, 1], a.navarray['80'][:, 2], clist[n])
        ax1.set_xlabel('Longitude (Degrees)')
        ax1.set_ylabel('Latitude (Degrees)')
        ax1.grid(True)
        ax2.plot(a.navarray['65'][:, 0], a.navarray['65'][:, 4], clist[n])
        ax2.set_ylabel('Heading (Degrees)')
        ax2.set_xlabel('Time (Seconds)')
        ax2.grid(True)
        if '104' in a.navarray:
            ax3.plot(a.navarray['104'][:, 0], a.navarray['104'][:, 1], clist[n])
        ax3.set_ylabel('Height (Meters)')
        ax3.grid(True)
        ax4.plot(a.navarray['65'][:, 0], a.navarray['65'][:, 1], clist[n])
        ax4.plot(a.navarray['65'][:, 0], a.navarray['65'][:, 2], clist[n] + '--')
        ax4.set_ylabel('Degress')
        ax4.legend(('Roll', 'Pitch'))
        ax4.grid(True)
        ax5.plot(a.navarray['65'][:, 0], a.navarray['65'][:, 3], clist[n])
        ax5.set_ylabel('Heave (Meters)')
        ax5.set_xlabel('Time (Seconds)')
        ax5.grid(True)
        n += 1
        if n >= len(clist):
            n = 0
        plt.draw()


def determine_good_chunksize(fil, minchunksize=40000000, max_chunks=20):
    """
    With given file, determine the best size of the chunk to read from it, given a minimum chunksize and a max
    number of chunks.

    Returns
    -------
    finalchunksize: int
        Size in bytes for the recommended chunk size

    """
    filesize = os.path.getsize(fil)

    # get number of chunks at minchunksize
    min_chunks = filesize / minchunksize
    if filesize <= minchunksize:
        # really small files can be below the given minchunksize in size.  Just use 4 chunks per in this case
        finalchunksize = int(filesize / 4)
        max_chunks = 4
    elif min_chunks <= max_chunks:
        # small files can use the minchunksize and be under the maxchunks per file
        # take remainder of min_chunks and glob it on to the chunksize
        #   if rounding ends up building chunks that leave out the last byte or something, don't worry, you are
        #   retaining the file handler and searching past the chunksize anyway
        max_chunks = np.floor(min_chunks)
        finalchunksize = int(minchunksize + (((min_chunks % 1) * minchunksize) / max_chunks))
    else:
        # Need a higher chunksize to get less than max_chunks chunks
        # Take chunks over max_chunks and increase chunksize to get to max_chunks
        overflowchunks = min_chunks - max_chunks
        finalchunksize = int(minchunksize + ((overflowchunks * minchunksize) / max_chunks))

    print('{}: Using {} chunks of size {}'.format(fil, max_chunks, finalchunksize))
    return finalchunksize


def return_chunked_fil(fil, startoffset=0, chunksize=20*1024*1024):
    """
    With given file, determine the best size of the chunk to read from it, given a minimum chunksize and a max
    number of chunks.

    Returns
    -------
    chunkfil: list of lists
        List containing [filepath, starting offset in bytes, end of chunk pointer in bytes] for each chunk to be read
        from file

    """
    filesize = os.path.getsize(fil)
    midchunks = [(t * chunksize + chunksize - startoffset) for t in range(int(filesize/chunksize))]

    # Sometimes this results in the last element being basically equal to the filesize when running it under client.map
    #    Do a quick check and just remove the element so you don't end up with a chunk that is like 10 bytes long
    if filesize - midchunks[len(midchunks) - 1] <= 1024:
        midchunks.remove(midchunks[len(midchunks) - 1])

    chunks = [startoffset] + midchunks + [filesize]
    chnkfil = []
    for chnk in chunks:
        if chunks.index(chnk) < len(chunks) - 1:  # list is a range, skip the last one as prev ended at the last index
            chnkfil.append([fil, chnk, chunks[chunks.index(chnk) + 1]])
    return chnkfil


def _checksum_all_bytes(bytes):
    # Calculate checksum by sum of bytes method
    bytes = bytearray(bytes)
    chk = sum(bytes) % 2 ** 16
    return np.uint16(chk)


def checksum_rawdatablock(rawdatablock):
    # checksum for bytes between STX and ETX
    # Assuming that the format of the datablock is:
    # 4 bytes datagram size, 1 byte STX -- DATA -- 1 byte ETX, 2 byte checksum
    # I.e. 5 bytes excluded at start and 3 bytes at the end
    return _checksum_all_bytes(rawdatablock[5:-3])


def main():
    if len(sys.argv) > 1:
        a = AllRead(sys.argv[0])
        a.mapfile(True)
        a.close()
    else:
        print("No filename provided.")


if __name__ == '__main__':
    main()
