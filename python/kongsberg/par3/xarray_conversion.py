import os
from glob import glob
import matplotlib.pyplot as plt
import webbrowser
from time import perf_counter
from sortedcontainers import SortedDict
import json
from datetime import datetime
from dask.distributed import wait
import xarray as xr
import numpy as np

from par3 import AllRead
from dask_helpers import dask_find_or_start_client, DaskProcessSynchronizer
from xarray_helpers import resize_zarr, my_xarr_to_zarr, xarr_to_netcdf, combine_xr_attributes, stack_nan_array, \
                           reform_nan_array

batch_read_enabled = True


def _xarr_is_bit_set(da, bitpos):
    """
    Check if bit is set using the Xarray DataArray and bit position.  Returns True if set.

    Parameters
    ----------
    da: xarray DataArray
    bitpos: integer offset representing bit posititon (3 to check 3rd bit)

    Returns
    -------
    chk: True if bitpos bit is set in val

    """
    try:
        # get integer value from bitpos and return True if set in num
        chk = da & (1 << (bitpos - 1))
    except TypeError:
        # won't work on dask arrays or floats, compute to get numpy
        chk = da.compute().astype(int) & (1 << (bitpos-1))

    return chk.astype(bool)


def _run_sequential_read(fildata):
    """
    Function for managing par.sequential_read_records.  Takes in .all files, outputs dict of records

    Parameters
    ----------
    fildata: list, contains chunk locations, see _batch_read_chunk_generation

    Returns
    -------
    pd.sequential_read_records: dict
        Dictionary where keys are the datagram type numbers and values are dicts of columns/rows from datagram

    """
    fil, offset, endpt = fildata
    pnds = AllRead(fil, start_ptr=offset, end_ptr=endpt)
    return pnds.sequential_read_records()


def _sequential_gather_max_beams(rec):
    """
    Takes in the output from read_sequential, outputs the maximum beam count from the beampointingangle dataset.  Used
    later for trimming to the max beam dimension across all datasets

    Parameters
    ----------
    rec: dict as returned by sequential_read_records

    Returns
    -------
    int, max of indices of the last beam across all records

    """
    last_real_beam = np.argmax(rec['78']['beampointingangle'] == 999, axis=1)
    return np.max(last_real_beam)


def _sequential_trim_to_max_beam_number(rec, max_beam_num):
    """
    Takes in the output from read_sequential, returns the records trimmed to the max beam number provided.

    Parameters
    ----------
    rec: dict as returned by sequential_read_records
    max_beam_num: int, max beam number across all records/workers

    Returns
    -------
    rec: original rec with beam dimension trimmed to max_beam_num

    """
    for dgram in rec:
        if dgram == '78':
            for rectype in rec[dgram]:
                if rec[dgram][rectype].ndim == 2:
                    rec[dgram][rectype] = rec[dgram][rectype][:, 0:max_beam_num]
    return rec


def _build_sector_mask(rec):
    """
    Range/Angle datagram is going to have duplicate times, which are no good for our Xarray dataset.  Duplicates are
    found at the same time but with different sectors, serial numbers (dualheadsystem) and/or frequency.  Split up the
    records by these three elements and use them as the sector identifier

    Parameters
    ----------
    rec: dict as returned by sequential_read_records

    Returns
    -------
    sector_ids = list, contains string identifiers for each sector, ex:
            ['40111_0_265000', '40111_0_275000', '40111_1_285000', '40111_1_290000', '40111_2_270000', '40111_2_280000']
    id_mask = list, index of where each sector_id identifier shows up in the data

    """
    # serial_nums = [rec['73'][x][0] for x in ['serial#', 'serial#2'] if rec['73'][x][0] != 0]
    serial_nums = list(np.unique(rec['78']['serial#']))
    sector_nums = [i for i in range(rec['78']['ntx'][0] + 1)]
    freqs = [f for f in np.unique(rec['78']['frequency'])]
    sector_ids = []
    id_mask = []

    for x in serial_nums:
        ser_mask = np.where(rec['78']['serial#'] == float(x))[0]
        for y in sector_nums:
            sec_mask = np.where(rec['78']['transmitsector#'] == float(y))[0]
            for z in freqs:
                f_mask = np.where(rec['78']['frequency'] == z)[0]
                totmask = [x for x in ser_mask if (x in sec_mask) and (x in f_mask)]
                if len(totmask) > 0:
                    # z (freq identifier) must be length 6 or else you end up with dtype=object that messes up
                    #    serialization later.  Zero pad to solve this
                    if len(str(int(z))) < 6:
                        z = '0' + str(int(z))
                    else:
                        z = str(int(z))
                    sector_ids.append(str(x) + '_' + str(y) + '_' + z)
                    id_mask.append(totmask)
    return sector_ids, id_mask


def _sequential_to_xarray(rec):
    """
    After running sequential read, this method will take in the dict of datagrams and return an xarray for rangeangle,
    attitude and navigation.  Three arrays for the three different time series we are interested in (full time series
    att and nav are useful later on)

    Parameters
    ----------
    rec: dict as returned by sequential_read_records

    Returns
    -------
    finalraw: Xarray.Dataset
        - coordinates include time, sector, beam
        - variables include soundspeed, ntx, nrx, nvalid, samplerate, tiltangle, signallength, delay, waveformid,
          beampointingangle, transmitsectorid, detectioninfo, qualityfactor, traveltime, mode, yawandpitchstabilization
        - attributes include profile_XXXXXXXXXXXXX, system_serial_number, secondary_system_serial_number,
          settings_XXXXXXXXXXX
    finalatt: Xarray.Dataset
        - coordinate is time
        - variables include roll, pitch, heave, heading
    finalnav: Xarray.Dataset
        - coordinate is time
        - variables include latitude, longitude, alongtrackvelocity, altitude
    """
    if '78' not in rec:
        print('No ping raw range/angle record found for chunk file')
        return
    recs_to_merge = {}
    alltims = np.unique(rec['78']['time'])  # after mask/splitting data, should get something for each unique time

    for r in rec:
        if r not in ['73', '85']:  # These are going to be added as attributes later
            recs_to_merge[r] = xr.Dataset()
            if r == '78':  # R&A is the only datagram we use that requires splitting by sector/serial#/freq
                ids, msk = _build_sector_mask(rec)  # get the identifiers and mask for each sector/serial#/freq
                for ky in rec[r]:
                    if ky not in ['time', 'serial#', 'frequency', 'transmitsector#']:
                        combined_sectors = []
                        for secid in ids:
                            idx = ids.index(secid)
                            datadtype = rec[r][ky].dtype

                            arr = np.array(rec['78'][ky][msk[idx]])  # that part of the record for the given sect_id
                            tim = rec['78']['time'][msk[idx]]

                            # currently i'm getting a one rec duplicate between chunked files...
                            if tim[-1] == tim[-2] and np.array_equal(arr[-1], arr[-2]):
                                # print('Found duplicate timestamp: {}, {}, {}'.format(r, ky, tim[-1]))
                                arr = arr[:-1]
                                tim = tim[:-1]

                            try:
                                arr = np.squeeze(np.array(arr), axis=1)  # Tx_data is handled this way to get 1D
                            except ValueError:
                                pass

                            # use the mask to build a zero-padded array with zeros for all times that have no data
                            paddedshp = list(arr.shape)
                            paddedshp[0] = alltims.shape[0]
                            padded_sector = np.zeros(paddedshp, dtype=datadtype)
                            padded_sector[np.where(np.isin(alltims, tim))] = arr
                            combined_sectors.append(padded_sector)

                        combined_sectors = np.array(combined_sectors)

                        # these records are by time/sector/beam.  Have to combine recs to build correct array shape
                        if ky in ['beampointingangle', 'transmitsectorid', 'detectioninfo', 'qualityfactor',
                                  'traveltime']:
                            beam_idx = [i for i in range(combined_sectors.shape[2])]
                            recs_to_merge[r][ky] = xr.DataArray(combined_sectors,
                                                                coords=[ids, alltims, beam_idx],
                                                                dims=['sector', 'time', 'beam'])
                        #  everything else isn't by beam, proceed normally
                        else:
                            if combined_sectors.ndim == 3:
                                combined_sectors = np.squeeze(combined_sectors, axis=2)
                            recs_to_merge[r][ky] = xr.DataArray(combined_sectors,
                                                                coords=[ids, alltims],
                                                                dims=['sector', 'time'])
            else:
                for ky in rec[r]:
                    if ky not in ['time']:
                        recs_to_merge[r][ky] = xr.DataArray(rec[r][ky], coords=[rec[r]['time']], dims=['time'])

    # occasionally get duplicate time values, possibly from overlap in parallel read
    _, index = np.unique(recs_to_merge['78']['time'], return_index=True)
    recs_to_merge['78'] = recs_to_merge['78'].isel(time=index)

    # take range/angle rec and merge runtime on to that index
    chk = True
    if '82' not in list(recs_to_merge.keys()):
        chk = False
    elif not np.any(rec['82']['time']):
        chk = False
    if not chk:
        # rec82 (runtime params) isn't mandatory, but all datasets need to have the same variables or else the
        #    combine_nested isn't going to work.  So if it isn't in rec (or it is empty) put in an empty dataset here
        #    to be interpolated through later after merge
        recs_to_merge['82'] = xr.Dataset(data_vars={'mode': (['time'], np.array([''])),
                                                    'yawandpitchstabilization': (['time'], np.array(['']))},
                                         coords={'time': np.array([float(recs_to_merge['78'].time[0])])})

    _, index = np.unique(recs_to_merge['82']['time'], return_index=True)
    recs_to_merge['82'] = recs_to_merge['82'].isel(time=index)
    recs_to_merge['82'] = recs_to_merge['82'].reindex_like(recs_to_merge['78'], method='nearest')
    finalraw = xr.merge([recs_to_merge[r] for r in recs_to_merge if r in ['78', '82']], join='inner')

    # attitude and nav are returned separately in their own datasets
    _, index = np.unique(recs_to_merge['65']['time'], return_index=True)
    finalatt = recs_to_merge['65'].isel(time=index)
    _, index = np.unique(recs_to_merge['110']['time'], return_index=True)
    finalnav = recs_to_merge['110'].isel(time=index)

    # Stuff that isn't of the same dimensions as the dataset are tacked on as attributes
    if '85' in rec:
        for t in rec['85']['time']:
            idx = np.where(rec['85']['time'] == t)
            profile = np.dstack([rec['85']['depth'][idx][0], rec['85']['soundspeed'][idx][0]])[0]
            finalraw.attrs['profile_{}'.format(int(t))] = json.dumps(profile.tolist())

    # add on attribute for installation parameters, basically the same way as you do for the ss profile, except it
    #   has no coordinate to index by.  Also, use json.dumps to avoid the issues with serializing lists/dicts with
    #   to_netcdf

    # I'm including these serial numbers for the dual/dual setup.  System is port, Secondary_system is starboard.  These
    #   are needed to identify pings and which offsets to use (TXPORT=Transducer0, TXSTARBOARD=Transducer1,
    #   RXPORT=Transducer2, RXSTARBOARD=Transudcer3)

    if '73' in rec:
        finalraw.attrs['system_serial_number'] = np.unique(rec['73']['serial#'])
        finalraw.attrs['secondary_system_serial_number'] = np.unique(rec['73']['serial#2'])
        for t in rec['73']['time']:
            idx = np.where(rec['73']['time'] == t)
            finalraw.attrs['settings_{}'.format(int(t))] = json.dumps(rec['73']['settings'][idx][0])

    # get the order of dimensions right
    finalraw = finalraw.transpose('time', 'sector', 'beam')

    return finalraw, finalatt, finalnav


def _divide_xarray_futs(xarrfuture, mode='range_angle'):
    """
    The return from _sequential_to_xarray is a future containing three xarrays.  Map this function to access that future
    and return the xarray specified with the mode keyword.

    Parameters
    ----------
    xarrfuture: xarray Dataset from _sequential_to_xarray

    Returns
    -------
    xarray Dataset: selected datatype specified by mode
    """
    idx = ['range_angle', 'attitude', 'navigation'].index(mode)
    return xarrfuture[idx]


def _divide_xarray_return_sector(xarrfuture, secid):
    """
    Take in a range_angle xarray Dataset and return just the secid sector

    Parameters
    ----------
    xarrfuture: xarray Dataset from _sequential_to_xarray

    Returns
    -------
    xarray Dataset: selected datatype specified by mode
    """
    if 'sector' not in xarrfuture.dims:
        return None
    if secid not in xarrfuture.sector.values:
        return None

    xarr_by_sec = xarrfuture.sel(sector=secid).drop('sector')

    # the where statement i do next seems to drop all the variable dtype info.  I seem to have to reset that
    varnames = list(xarr_by_sec.variables.keys())
    xarr_valid_pings = xarr_by_sec.where(xarr_by_sec.ntx != 0, drop=True)
    for v in varnames:
        xarr_valid_pings[v] = xarr_valid_pings[v].astype(xarr_by_sec[v].dtype)

    return xarr_valid_pings


def _divide_xarray_indicate_empty_future(fut):
    """

    Parameters
    ----------
    fut: xarray DataSet: Dataset if fut is valid, NoneType if empty

    Returns
    -------
    bool, True if fut is data, False if not

    """
    if fut is None:
        return False
    elif np.array_equal(fut.time.values, np.array([])):
        return False
    else:
        return True


def _return_xarray_mintime(xarrs):
    """
    Access xarray object and return the length of the time dimension.
    """
    return float(xarrs.time.min())


def _return_xarray_sectors(xarrs):
    """
    Return the sector names for the given xarray object
    """
    return xarrs.sector.values


def _return_xarray_timelength(xarrs):
    """
    Access xarray object and return the length of the time dimension.
    """
    return xarrs.dims['time']


def _assess_need_for_split_correction(cur_xarr, next_xarr):
    """
    Taking blocks from workers, if the block after the current one has a start time equal to the current end time,
    you have a ping that stretches across the block.  You can't end up with duplicate times, so flag this one as needing
    correction.

    Parameters
    ----------
    cur_xarr: xarray Dataset, the current one in queue for assessment
    next_xarr: xarray Dataset, the next one in time order

    Returns
    -------
    True if needing split correction

    """
    if next_xarr is not None:
        cur_rec = cur_xarr.isel(time=-1)
        try:
            next_rec = next_xarr.isel(time=0)
        except ValueError:
            # get here if you selected the first record prior to this method
            next_rec = next_xarr
        # if the first time on the next array equals the last one on the current one....you gotta fix the
        #    sector-pings
        if float(cur_rec.time) == float(next_rec.time):
            return True
        else:
            return False
    else:
        return False


def _add_empty_sectors(cur_xarr, expected_secs):
    """
    Some chunks will not have pings from certain frequencies/sectors/SN#s.  We want each chunk to have the same
    dimensions, so pad the chunk with zerod out values for the missing ones.

    Parameters
    ----------
    cur_xarr: xarray Dataset, the current one in queue for assessment
    expected_secs: list of str, the total sectors that should be present in each block

    Returns
    -------
    final: xarray Dataset, the padded cur_xarr

    """
    to_concat = []
    override_vars = [x for x in cur_xarr.variables if 'sector' not in cur_xarr[x].dims and x not in ['time', 'beam']]

    for sec in expected_secs:
        if sec in cur_xarr.sector:
            to_concat.append(cur_xarr.sel(sector=sec))
        else:
            new_zerod_xarr = xr.zeros_like(cur_xarr.isel(sector=0))
            new_zerod_xarr['sector'] = xr.DataArray(sec)
            to_concat.append(new_zerod_xarr)
    final = xr.concat(to_concat, dim='sector').transpose('time', 'sector', 'beam')

    for v in override_vars:
        final[v] = cur_xarr[v]

    return final


def _correct_for_splits_between_sectors(cur_xarr, next_xarr, trim_the_array, expected_secs):
    """
    We split up the files into chunks for processing on individual workers, then split the data78 recs into
    sector-pings where each sector/freq is stored like a ping.  Finally, combine the chunks so they are all the
    same length.  Problem with this is that making chunks all the same length means you might split sectors
    between chunks, which you cant fix well with changing the blocksize as that is constant across blocks.  Xarray
    combine_nested will not combine these well and you end up with two times, one for each sector with the other
    sector being empty.  Here we try and fix these xarray datasets and merge the sectors across the gap.

    Parameters
    ----------
    cur_xarr: xarray Dataset, the current one in queue for assessment
    next_xarr: xarray Dataset, the next one in time order
    trim_the_array: bool, if True we remove the first time record (see _assess_need_for_split_correction)
    expected_secs: list of str, the total sectors that should be present in each block

    Returns
    -------
    cur_xarr: xarray Dataset, corrected version of cur_xarr

    """
    if expected_secs is not None:
        if not np.array_equal(sorted(list(cur_xarr.sector.values)), sorted(expected_secs)):
            print('Block {}:{} is missing sector/freq/SN identifier {}, adding...'.format(float(cur_xarr.time.min()),
                                                                                          float(cur_xarr.time.max()),
                                                                                          [s for s in expected_secs if
                                                                                           s not in cur_xarr.sector]))
            cur_xarr = _add_empty_sectors(cur_xarr, expected_secs)
    if next_xarr is not None:
        cur_rec = cur_xarr.isel(time=-1)
        try:
            next_rec = next_xarr.isel(time=0)
        except ValueError:
            # get here if you selected the first record prior to this method
            next_rec = next_xarr
        # if the first time on the next array equals the last one on the current one....you gotta fix the
        #    sector-pings
        if float(cur_rec.time) == float(next_rec.time):
            sector_names = next_rec.sector.values
            # the sector that is missing in cur_rec
            sec_tocopy_idx = [list(sector_names).index(sec) for sec in sector_names if next_rec.sel(sector=sec)['ntx']]
            currec_idx = []
            for idx in sec_tocopy_idx:
                currec_idx.append(list(cur_rec.sector.values).index(next_rec.isel(sector=idx).sector.values))

            # https://github.com/pydata/xarray/issues/2180
            #    can't assign values using sel or isel, see also
            #    https://stackoverflow.com/questions/48229860/set-values-using-name-index-in-xarray
            for ct, sec in enumerate(sec_tocopy_idx):
                for k, v in next_rec.isel(sector=sec).items():
                    if cur_xarr[k].ndim == 3:
                        cur_xarr[k][-1, currec_idx[ct], :] = v
                    elif cur_xarr[k].ndim == 2:
                        cur_xarr[k][-1, currec_idx[ct]] = v
                    else:
                        cur_xarr[k][-1] = v
                print('merging {} and {}, modifying last ping of {} tims'.format(
                    str(next_rec.isel(sector=sec).sector.values),
                    str(cur_rec.isel(sector=currec_idx[ct]).sector.values),
                    len(cur_xarr.time)))
    if trim_the_array:
        print('remove first: {} to {}'.format(len(cur_xarr.time), slice(1, len(cur_xarr.time))))
        cur_xarr = cur_xarr.isel(time=slice(1, len(cur_xarr.time)))
    return cur_xarr


def _return_xarray_constant_blocks(xlens, xarrfutures, rec_length=5000):
    """
    Sequential read operates on a file level.  Chunks determined for what makes sense in terms of distributed
    processing.  For use with netcdf/zarr datastores, we'd ideally like to have equal length chunks across workers
    (where each worker writes to a different netcdf file/zarr group area).  This method will build out lists of
    xarray futures for each worker to combine in order to get constant chunk size equal to the rec_length parameter.

    Parameters
    ----------

    xlens: list of int, length of the time dimension for each array, same order as xarrfutures
    xarrfutures: list of dask futures, future represents xarray dataset for chunk
    rec_length: int, length of time dimension for output block

    Returns
    -------
    newxarrs = list: list of lists where each inner list is [start, end, xarray future].  For use with
                     _merge_constant_blocks, start and end correspond to the time dimension
    totallen = int: total number of time values across all chunks

    """
    newxarrs = []
    cur_req = rec_length
    bufr = []
    totallen = 0
    for ct, l in enumerate(xlens):
        # range angle length from this worker/future
        arrlen = l
        # Just in case you get to zero, start over
        if cur_req == 0:
            cur_req = rec_length
        # if the length of the rec is less than the desired blocksize,
        #    add it to the buffer and move on
        if arrlen <= cur_req:
            bufr.append([0, arrlen, xarrfutures[ct]])
            totallen += arrlen
            cur_req -= arrlen
        # if the length is greater than blocksize, add enough to get blocksize
        #    and attach the rest to overflow for the next block
        elif arrlen > cur_req:
            start_idx = 0
            while arrlen > cur_req:
                bufr.append([start_idx, cur_req, xarrfutures[ct]])
                newxarrs.append(bufr)
                bufr = []
                totallen += cur_req - start_idx
                start_idx = cur_req
                cur_req += rec_length
            if start_idx:
                cur_req -= rec_length

            bufr = [[cur_req, arrlen, xarrfutures[ct]]]
            totallen += (arrlen - cur_req)
            cur_req = rec_length - (arrlen - cur_req)
    newxarrs.append(bufr)
    return newxarrs, totallen


def _merge_constant_blocks(newblocks):
    """
    Accepts output from _return_xarray_constant_blocks and performs a nested concat/merge on given blocks.

    Parameters
    ----------
    newblocks: list, [time_start, time_end, xarray Dataset] where time_start and time_end are ints for time indexes
                [0, 25, xarray Dataset] selects the dataset for [0,25] time indices

    Returns
    -------
    finalarr = xarray Dataset: all blocks merged along time dimension
    """
    xarrs = [i[2].isel(time=slice(i[0], i[1])) for i in newblocks]
    finalarr = xr.combine_nested(xarrs, 'time')
    return finalarr


def gather_dataset_attributes(dataset):
    """
    Return the attributes within an Xarray DataSet

    Parameters
    ----------
    dataset: Xarray DataSet

    Returns
    -------
    dict, attributes within dataset

    """
    return dataset.attrs


def combine_xarrs(xarrs):
    """
    Takes in a list of xarrays and concatenates/merges on time.

    Will not check to see if the data fits in memory, so buyer beware.

    Parameters
    ----------
    xarrs: xarray Dataset from _sequential_to_xarray

    """
    range_angle_xarrs = [x[0] for x in xarrs]
    att_xarrs = [x[1] for x in xarrs]
    nav_xarrs = [x[2] for x in xarrs]
    ra_attrs = combine_xr_attributes(range_angle_xarrs)

    range_angle = xr.combine_nested(range_angle_xarrs, 'time')
    range_angle.attrs = ra_attrs
    attitude = xr.combine_nested(att_xarrs, 'time')
    navigation = xr.combine_nested(nav_xarrs, 'time')
    return range_angle, attitude, navigation


def xarrs_to_appended_netcdf(xarrs, basepath):
    """
    Takes in a list of xarrays and creates/appends to single nc files.
    Expects xarrays to be in order of range_angle, attitude, navigation

    First run generates netcdf file.

    Parameters
    ----------
    xarrs: xarray Dataset from _sequential_to_xarray
    basepath: str, location for netcdf data stores

    """
    range_angle_xarrs = [x[0] for x in xarrs]
    att_xarrs = [x[1] for x in xarrs]
    nav_xarrs = [x[2] for x in xarrs]
    ra_attrs = combine_xr_attributes(range_angle_xarrs)

    range_angle_pth = os.path.join(basepath, 'range_angle.nc')
    attitude_pth = os.path.join(basepath, 'attitude.nc')
    navigation_pth = os.path.join(basepath, 'navigation.nc')

    for ra in range_angle_xarrs:
        if not os.path.exists(range_angle_pth):
            ra.attrs = ra_attrs
            ra.to_netcdf(range_angle_pth, mode='w', format='NETCDF4', engine='netcdf4')
        else:
            ra.to_netcdf(range_angle_pth, mode='a', format='NETCDF4', engine='netcdf4')

    for att in att_xarrs:
        if not os.path.exists(attitude_pth):
            att.to_netcdf(attitude_pth, mode='w', format='NETCDF4', engine='netcdf4')
        else:
            att.to_netcdf(attitude_pth, mode='a', format='NETCDF4', engine='netcdf4')

    for nav in nav_xarrs:
        if not os.path.exists(navigation_pth):
            nav.to_netcdf(navigation_pth, mode='w', format='NETCDF4', engine='netcdf4')
        else:
            nav.to_netcdf(navigation_pth, mode='a', format='NETCDF4', engine='netcdf4')
    return range_angle_pth, attitude_pth, navigation_pth


def _closest_prior_key_value(tstmps, key):
    """
    With given list of timestamps, return the one that is closest to the key but also prior to the key

    Parameters
    ----------
    tstmps = list, floats of timestamps
    key = float timestamp

    Returns
    -------
    tstmp = float, timestamp that is closest and prior to key

    """

    try:
        sett_tims = np.array([float(x) for x in tstmps])
    except ValueError:
        print('Unable to generate list of floats from: {}'.format(tstmps))
        return None

    tim = float(key)
    difs = tim - sett_tims
    difs[difs < 0] = np.nan
    closest_tim = sett_tims[np.nanargmin(difs)]
    return closest_tim


def _closest_key_value(tstmps, key):
    """
    With given list of timestamps, return the one that is closest to the key

    Parameters
    ----------
    tstmps = list, floats of timestamps
    key = float timestamp

    Returns
    -------
    tstmp = float, timestamp that is closest to the key

    """

    try:
        sett_tims = np.array([float(x) for x in tstmps])
    except ValueError:
        print('Unable to generate list of floats from: {}'.format(tstmps))
        return None

    tim = float(key)
    difs = tim - sett_tims
    closest_tim = sett_tims[np.nanargmin(difs)]
    return closest_tim


def batch_read_configure_options():
    """
    Generate the parameters that drive the data conversion.  Chunksize for size of zarr written chunks,
    combine_attributes as a bool to tell the system to look for attributes within that xarray object, and
    output_arrs/final_pths/final_attrs to hold the data as it is processed.

    Returns
    -------
    opts: dict, options for batch read process

    """
    opts = {
        'range_angle': {'chunksize': 5000, 'combine_attributes': True, 'output_arrs': [], 'final_pths': None,
                        'final_attrs': None},
        'attitude': {'chunksize': 20000, 'combine_attributes': False, 'output_arrs': [], 'final_pths': None,
                     'final_attrs': None},
        'navigation': {'chunksize': 50000, 'combine_attributes': False, 'output_arrs': [], 'final_pths': None,
                       'final_attrs': None}}
    return opts


class BatchRead:
    """
    BatchRead - Kongsberg .all data converter using dask infrastructure
    pass in filfolder full of .all files (or the path to a single file), call read(), and gain access to xarray Dataset
    object

    BatchRead is stored internally using the following conventions:
    - X = + Forward, Y = + Starboard, Z = + Down
    - roll = + Port Up, pitch = + Bow Up, gyro = + Clockwise

    self.raw_rangeangle will be dask delayed object, if you need to pull locally, use the self.raw_rangeangle.compute()
      method.

    Parameters
    ----------
    filfolder: string, Folder of .all files
    address: string, None for setting up local cluster, IP:Port for remote dask server session
    minchunksize: int, minimum size of chunks you want to split files in to
    max_chunks: int, maximum chunks per file

    >> from xarray_conversion import BatchRead

    >> cnverted = BatchRead(r'C:\\collab\\dasktest\\data_dir\\EM2040')

    Started local cluster client...

    >> cnverted.read()

    Running Kongsberg .all converter...
    - C:\\collab\\dasktest\\data_dir\\EM2040\\0001_20170822_144548_S5401_X.all: Using 6 chunks of size 44296946
    - C:\\collab\\dasktest\\data_dir\\EM2040\\0003_20170822_150341_S5401_X.all: Using 8 chunks of size 40800594
    - C:\\collab\\dasktest\\data_dir\\EM2040\\0005_20170822_152146_S5401_X.all: Using 7 chunks of size 41886286
    - C:\\collab\\dasktest\\data_dir\\EM2040\\0007_20170822_153922_S5401_X.all: Using 6 chunks of size 43280232
    - C:\\collab\\dasktest\\data_dir\\EM2040\\0009_20170822_155626_S5401_X.all: Using 7 chunks of size 42632467
    - C:\\collab\\dasktest\\data_dir\\EM2040\\0010_20170822_160627_S5401_X.all: Using 5 chunks of size 41804621
    - C:\\collab\\dasktest\\data_dir\\EM2040\\0011_20170822_161430_S5401_X.all: Using 7 chunks of size 43407640
    - C:\\collab\\dasktest\\data_dir\\EM2040\\0012_20170822_162915_S5401_X.all: Using 5 chunks of size 43478335

    Rebalancing 92186 total range_angle records across 19 blocks of size 5000
    Rebalancing 410484 total attitude records across 21 blocks of size 20000
    Rebalancing 828315 total navigation records across 17 blocks of size 50000
    Distributed conversion complete: 85.24223939999999s
    Found 1 total Installation Parameters entr(y)s
    Constructed offsets successfully
    Translated realtime parameters record successfully

    >> cnverted.raw_rangeangle
    <xarray.Dataset>
    - Dimensions:                   (beam: 400, sector: 3, time: 92186)
    - Coordinates:
      * beam                      (beam) int32 0 1 2 3 4 5 ... 395 396 397 398 399
      * sector                    (sector) <U14 '40107_0_260000' ... '40107_2_290000'
      * time                      (time) float64 1.503e+09 1.503e+09 ... 1.503e+09
    - Data variables:
        beampointingangle         (time, sector, beam) float64 dask.array<chunksize=(5000, 3, 400), meta=np.ndarray>
        delay                     (time, sector) float64 dask.array<chunksize=(5000, 3), meta=np.ndarray>
        detectioninfo             (time, sector, beam) float64 dask.array<chunksize=(5000, 3, 400), meta=np.ndarray>
        mode                      (time) <U1 dask.array<chunksize=(5000,), meta=np.ndarray>
        nrx                       (time, sector) float64 dask.array<chunksize=(5000, 3), meta=np.ndarray>
        ntx                       (time, sector) float64 dask.array<chunksize=(5000, 3), meta=np.ndarray>
        nvalid                    (time, sector) float64 dask.array<chunksize=(5000, 3), meta=np.ndarray>
        qualityfactor             (time, sector, beam) float64 dask.array<chunksize=(5000, 3, 400), meta=np.ndarray>
        samplerate                (time, sector) float64 dask.array<chunksize=(5000, 3), meta=np.ndarray>
        signallength              (time, sector) float64 dask.array<chunksize=(5000, 3), meta=np.ndarray>
        soundspeed                (time, sector) float64 dask.array<chunksize=(5000, 3), meta=np.ndarray>
        tiltangle                 (time, sector) float64 dask.array<chunksize=(5000, 3), meta=np.ndarray>
        transmitsectorid          (time, sector, beam) float64 dask.array<chunksize=(5000, 3, 400), meta=np.ndarray>
        traveltime                (time, sector, beam) float64 dask.array<chunksize=(5000, 3, 400), meta=np.ndarray>
        waveformid                (time, sector) float64 dask.array<chunksize=(5000, 3), meta=np.ndarray>
        yawandpitchstabilization  (time) <U1 dask.array<chunksize=(92186,), meta=np.ndarray>
    - Attributes:
        - kongsberg_files:                 ['0001_20170822_144548_S5401_M.all', '00...
        - profile_1503411780:              [[0.0, 1517.0999755859375], [0.15, 1517....
        - profile_1503419100:              [[0.0, 1519.0999755859375], [0.15, 1519....
        - secondary_system_serial_number:  [0]
        - settings_1503413148:             {"waterline_vertical_location": "-1.010"...
        - survey_number:                   ['H12990_M']
        - system_serial_number:            [40107]

    >> cnverted.raw_att

    <xarray.Dataset>
    - Dimensions:  (time: 410484)
    - Coordinates:
      * time     (time) float64 1.503e+09 1.503e+09 ... 1.503e+09 1.503e+09
    - Data variables:
        heading  (time) float64 dask.array<chunksize=(20000,), meta=np.ndarray>
        heave    (time) float64 dask.array<chunksize=(20000,), meta=np.ndarray>
        pitch    (time) float64 dask.array<chunksize=(20000,), meta=np.ndarray>
        roll     (time) float64 dask.array<chunksize=(20000,), meta=np.ndarray>

    >> cnverted.raw_nav

    <xarray.Dataset>
    - Dimensions:             (time: 828315)
    - Coordinates:
      * time                (time) float64 1.503e+09 1.503e+09 ... 1.503e+09
    - Data variables:
        alongtrackvelocity  (time) float64 dask.array<chunksize=(50000,), meta=np.ndarray>
        altitude            (time) float64 dask.array<chunksize=(50000,), meta=np.ndarray>
        latitude            (time) float64 dask.array<chunksize=(50000,), meta=np.ndarray>
        longitude           (time) float64 dask.array<chunksize=(50000,), meta=np.ndarray>

    """

    def __new__(cls, filfolder, address=None, minchunksize=40000000, max_chunks=20, filtype='zarr'):
        if not batch_read_enabled:
            print('Dask and Xarray are required dependencies to run BatchRead.  Please ensure you have these modules ' +
                  'first.')
            return None
        else:
            return super(BatchRead, cls).__new__(cls)

    def __init__(self, filfolder, address=None, minchunksize=40000000, max_chunks=20, filtype='zarr'):
        self.filfolder = filfolder
        self.filtype = filtype
        self.convert_minchunksize = minchunksize
        self.convert_maxchunks = max_chunks
        self.address = address
        self.raw_rangeangle = None
        self.raw_att = None
        self.raw_nav = None

        self.readsuccess = False
        self.client = dask_find_or_start_client(address=self.address)

        # misc
        self.extents = None

        # install parameters
        self.sonartype = None
        self.xyzrph = None

        self.ky_data73_sonar_translator = {'em122': [None, 'tx', 'rx', None], 'em302': [None, 'tx', 'rx', None],
                                           'em710': [None, 'tx', 'rx', None], 'em2040': [None, 'tx', 'rx', None],
                                           'em2040_dual_rx': [None, 'tx', 'rx_port', 'rx_stbd'],
                                           'em2040_dual_tx': ['tx_port', 'tx_stbd', 'rx_port', 'rx_stbd'],
                                           # EM2040c is represented in the .all file as em2045
                                           'em2045': [None, 'txrx', None, None],
                                           'em3002': [None, 'tx', 'rx', None],
                                           'em2040p': [None, 'txrx', None, None],
                                           'me70bo': ['txrx', None, None, None]}
        self.ky_data73_install_modifier = {'em2040': {'rx': {'0': {'x': 0.011, 'y': 0.0, 'z': -0.006},
                                                             '1': {'x': 0.011, 'y': 0.0, 'z': -0.006},
                                                             '2': {'x': 0.011, 'y': 0.0, 'z': -0.006}},
                                                      'tx': {'0': {'x': 0.0, 'y': -0.0554, 'z': -0.012},
                                                             '1': {'x': 0.0, 'y': 0.0131, 'z': -0.006},
                                                             '2': {'x': 0.0, 'y': 0.0554, 'z': -0.012}}},
                                           'em2045': {'rx': {'0': {'x': -0.0455, 'y': 0.0, 'z': -0.006},
                                                             '1': {'x': -0.0455, 'y': 0.0, 'z': -0.006}},
                                                      'tx': {'0': {'x': 0.0038, 'y': 0.040, 'z': -0.006},
                                                             '1': {'x': 0.0038, 'y': 0.040, 'z': -0.006}}},
                                           'em2040p': {'rx': {'0': {'x': 0.204, 'y': 0.0, 'z': -0.0315},
                                                              '1': {'x': 0.204, 'y': 0.0, 'z': -0.0315},
                                                              '2': {'x': 0.204, 'y': 0.0, 'z': -0.0315}},
                                                       'tx': {'0': {'x': 0.002, 'y': -0.1042, 'z': -0.0149},
                                                              '1': {'x': 0.002, 'y': 0.0, 'z': -0.006},
                                                              '2': {'x': 0.002, 'y': 0.1042, 'z': -0.0149}}},
                                           }

    def read(self, by_sector=True):
        """
        Run the batch_read method on all available lines, writes to datastore (netcdf/zarr depending on self.filtype),
        and loads the data back into the class as self.raw_rangeangle, self.att, self.nav.

        If data loads correctly, builds out the self.xyzrph attribute and translates the runtime parameters to a usable
        form.

        Parameters
        ----------
        by_sector: bool, True if you want to break up the input files into separate outputs based on sector
        """
        if self.filtype not in ['netcdf', 'zarr']:
            raise NotImplementedError(self.filtype + ' is not a supported format.')

        if by_sector:
            final_pths = self.batch_read_by_sector(self.filtype)
        else:
            final_pths = self.batch_read(self.filtype)
        if final_pths is not None:
            if self.filtype == 'netcdf':
                self.read_from_netcdf_fils(final_pths['range_angle'], final_pths['attitude'], final_pths['navigation'])
            elif self.filtype == 'zarr':
                self.read_from_zarr_fils(final_pths['range_angle'], final_pths['attitude'], final_pths['navigation'])
        else:
            raise ValueError('Unable to start/connect to the Dask distributed cluster.')

        if self.raw_rangeangle is not None:
            self.readsuccess = True
            self.build_offsets()

    def read_from_netcdf_fils(self, range_angle_pths, attitude_pths, navigation_pths):
        """
        Read from the generated netCDF files constructed with read()

        **Currently some issues with open_mfdataset that I've not resolved.  Using it with the dask distributed
        cluster active results in worker errors/hdf errors.  Using it without the distributed cluster works fine.  So
        annoying.  I'm sticking to the zarr stuff for now, distributed parallel read/writes appear to work there after
        I built my own writer.**

        Parameters
        ----------
        range_angle_pths: list, paths to the range_angle netcdf files
        attitude_pths: str, path to the attitude netcdf files
        navigation_pths: str, path to the navigation netcdf files

        """
        # sort input pths by type, currently a list by idx
        self.raw_rangeangle = xr.open_mfdataset(range_angle_pths, chunks={}, concat_dim='time', combine='nested')
        self.raw_att = xr.open_mfdataset(attitude_pths, chunks={}, concat_dim='time', combine='nested')
        self.raw_nav = xr.open_mfdataset(navigation_pths, chunks={}, concat_dim='time', combine='nested')

    def read_from_zarr_fils(self, range_angle_pth, attitude_pth, navigation_pth):
        """
        Read from the generated zarr datastore constructed with read()

        All the keyword arguments set to False are there to correctly read the saved zarr arrays.  Mask_and_scale i've
        yet to configure properly, it will replace values equal to the fill_value attribute with NaN.  Even when
        fill_value is non-zero, it seems to replace zeros with NaN.  Setting it to false prevents this.  You can read
        more here:  http://xarray.pydata.org/en/stable/generated/xarray.open_zarr.html

        Parameters
        ----------
        range_angle_pth: str, path to the range_angle zarr group
        attitude_pth: str, path to the attitude zarr group
        navigation_pth: str, path to the navigation zarr group

        """
        self.raw_rangeangle = []
        if type(range_angle_pth) != list:
            range_angle_pth = [range_angle_pth]
        if type(attitude_pth) != list:
            attitude_pth = [attitude_pth]
        if type(navigation_pth) != list:
            navigation_pth = [navigation_pth]

        for ra in range_angle_pth:
            xarr = xr.open_zarr(ra, synchronizer=DaskProcessSynchronizer(ra),
                                mask_and_scale=False, decode_coords=False, decode_times=False,
                                decode_cf=False, concat_characters=False)

            try:
                # this is for by sector conversion, so you retain the sector identifier when the sector dimension is
                #     dropped
                xarr.attrs['sector_identifier'] = os.path.splitext(os.path.split(ra)[1])[0][12:]
            except ValueError:
                pass

            self.raw_rangeangle.append(xarr)
        self.raw_att = xr.open_zarr(attitude_pth[0],
                                    synchronizer=DaskProcessSynchronizer(attitude_pth[0]),
                                    mask_and_scale=False, decode_coords=False, decode_times=False, decode_cf=False,
                                    concat_characters=False)
        self.raw_nav = xr.open_zarr(navigation_pth[0],
                                    synchronizer=DaskProcessSynchronizer(navigation_pth[0]),
                                    mask_and_scale=False, decode_coords=False, decode_times=False, decode_cf=False,
                                    concat_characters=False)

    def _batch_read_file_setup(self):
        """
        With given path to folder of kongsberg .all files (self.filfolder), return the paths to the individual .all
        files and the path to the output folder for the netcdf/zarr converted files.

        Returns
        -------
        fils: list, paths to all the kongsberg .all files in self.filfolder
        converted_pth: str, path to the newly created converted folder where zarr/netcdf files will go

        """
        if os.path.isdir(self.filfolder):
            fils = glob(os.path.join(self.filfolder, '*.all'))
        elif os.path.isfile(self.filfolder):
            fils = [self.filfolder]
            self.filfolder = os.path.dirname(self.filfolder)

        converted_pth = os.path.join(self.filfolder, 'converted')
        if os.path.exists(converted_pth):
            converted_pth = os.path.join(self.filfolder, 'converted_{}'.format(datetime.now().strftime('%H%M%S')))
        os.makedirs(converted_pth)
        return fils, converted_pth

    def _batch_read_chunk_generation(self, fils):
        """
        For each .all file, determine a good chunksize for the distributed read/processing and build a list with
        files, start bytes and end bytes.

        Parameters
        ----------
        fils: list, list of paths to .all files

        Returns
        -------
        chnks_flat: list, list of chunks given as [filepath, starting offset in bytes, end of chunk pointer in bytes]

        """
        chnks = []
        for f in fils:
            chnks.append(return_chunked_fil(f, 0, determine_good_chunksize(f, self.convert_minchunksize,
                                                                           self.convert_maxchunks)))
        # chnks_flat is now a list of lists representing chunks of each file
        chnks_flat = [c for subc in chnks for c in subc]
        return chnks_flat

    def _batch_read_merge_blocks(self, input_xarrs, datatype, chunksize):
        """
        Take the blocks workers have been working on up to this point (from reading raw files) and reorganize them
        into equal blocks that are of a size that makes sense later down the line.  Larger blocks for processing than
        the smaller ones used during file access.

        Parameters
        ----------
        input_xarrs: list, xarray objects representing data read from raw files
        datatype: string, one of range_angle, attitude, navigation
        chunksize: int, size of new chunks, see batch_read_configure_options

        Returns
        -------
        output_arrs: list, futures representing data merged according to balanced_data
        chunksize: int, size of chunks operated on by workers, shortened if greater than the total size
        totallength: int, total length of blocks

        """
        xlens = self.client.gather(self.client.map(_return_xarray_timelength, input_xarrs))
        balanced_data, totallength = _return_xarray_constant_blocks(xlens, input_xarrs, rec_length=chunksize)
        print('Rebalancing {} total {} records across {} blocks of size {}'.format(totallength, datatype,
                                                                                   len(balanced_data), chunksize))

        # if the chunksize is greater than the total amount of records, adjust to the total amount of records.
        #   This is to prevent empty values being written to the zarr datastore
        if (len(balanced_data) == 1) and (totallength < chunksize):
            print('Less values found than chunk size, resizing final chunksize from {} to {}'.format(chunksize,
                                                                                                     totallength))
            chunksize = totallength

        # merge old arrays to get new ones of chunksize
        output_arrs = self.client.map(_merge_constant_blocks, balanced_data)
        del balanced_data
        return output_arrs, chunksize, totallength

    def _batch_read_sequential_and_trim(self, chnks_flat):
        """
        Kongsberg sonars have dynamic sectors, where the number of beams in each sector varies over time.  As a result,
        we pad the beam arrays to a total size of 400.  At this point though, since we can now learn max beams across
        sectors/time, we want to trim to just the max beam count in each sector.

        Parameters
        ----------
        chnks_flat: output from _batch_read_chunk_generation describing the byte offset and length of chunks

        Returns
        -------
        newrecfutures: list, dict for all records/datagrams in .all file

        """
        # recfutures is a list of futures representing dicts from sequential read
        recfutures = self.client.map(_run_sequential_read, chnks_flat)
        maxnums = self.client.map(_sequential_gather_max_beams, recfutures)
        maxnum = np.max(self.client.gather(maxnums))
        newrecfutures = self.client.map(_sequential_trim_to_max_beam_number, recfutures, [maxnum] * len(recfutures))
        del recfutures, maxnums, maxnum
        return newrecfutures

    def _batch_read_correct_block_boundaries(self, input_xarrs, totalsecs=None):
        """
        See _correct_for_splits_between_sectors.  Handle cases where sectors are split across worker blocks/files and
        must be repaired in order to avoid duplicate timestamps.

        Parameters
        ----------
        input_xarrs: list, xarray Dataset object from _sequential_to_xarray
        totalsecs: list, total sectors in file(s), include here if you want to add empty sectors to each input_xarr such
                   that the total sectors across xarrs is equal

        Returns
        -------
        input_xarrs: list, xarray Dataset futures representing the input_xarrs corrected for splits between files/blocks

        """
        base_xarrfut = input_xarrs
        next_xarrfut = input_xarrs[1:] + [None]
        trim_arr = self.client.map(_assess_need_for_split_correction, base_xarrfut, next_xarrfut)
        input_xarrs = self.client.map(_correct_for_splits_between_sectors, base_xarrfut, next_xarrfut,
                                      [trim_arr[-1]] + trim_arr[:-1], [totalsecs] * len(base_xarrfut))
        del trim_arr, base_xarrfut, next_xarrfut
        return input_xarrs

    def _batch_read_sort_futures_by_time(self, input_xarrs):
        """
        Futures should retain input order (order passed to mapped function), but I've found that sorting by time will
        sometimes catch instances where futures are not sorted.

        Parameters
        ----------
        input_xarrs: list, xarray Dataset object from _sequential_to_xarray

        Returns
        -------
        input_xarrs: list, xarray Dataset futures object from _sequential_to_xarray, sorted by time

        """
        # sort futures before doing anything else
        mintims = self.client.gather(self.client.map(_return_xarray_mintime, input_xarrs))
        sort_mintims = sorted(mintims)
        if mintims != sort_mintims:
            print('Resorting futures to time index: {}'.format(sort_mintims))
            new_futzips = sorted(zip(mintims, input_xarrs))
            input_xarrs = [fut for tim, fut in new_futzips]
            del new_futzips
        del mintims, sort_mintims
        return input_xarrs

    def _batch_read_return_xarray_by_sector(self, input_xarrs, sec):
        """
        Take in the sector identifier (sec) and only return xarray objects with that sector in them, also selecting
        only that sector.

        Parameters
        ----------
        input_xarrs: list, xarray Dataset object from _sequential_to_xarray
        sec: str, sector name

        Returns
        -------
        valid_input_xarrs: list, input_xarrs selected sector, with xarrs dropped if they didn't contain the sector

        """
        list_of_secs = [sec] * len(input_xarrs)
        input_xarrs_by_sec = self.client.map(_divide_xarray_return_sector, input_xarrs, list_of_secs)
        empty_mask = self.client.gather(self.client.map(_divide_xarray_indicate_empty_future, input_xarrs_by_sec))
        valid_input_xarrs = [in_xarr for cnt, in_xarr in enumerate(input_xarrs_by_sec) if empty_mask[cnt]]
        return valid_input_xarrs

    def _batch_read_write(self, output_mode, datatype, opts, converted_pth, totallen, secid=None):
        """
        Write out the xarray Dataset(s) to the specified data storage type

        Parameters
        ----------
        output_mode: str, identifies the type of data storage format
        datatype: str, one of range_angle, attitude, navigation
        opts: list, output of batch_read_configure_options, contains output arrays and options for writing
        converted_pth: str, path to the output datastore
        totallen: int, total length of the time dimension, used to resize final array
        secid: str, sector name if writing by sector to zarr datastore

        Returns
        -------

        """
        fpths = ''
        if output_mode == 'netcdf':
            fpths = self._batch_read_write_netcdf(datatype, opts, converted_pth)
        elif output_mode == 'zarr':
            fpths = self._batch_read_write_zarr(datatype, opts, converted_pth, secid=secid)

        if fpths:
            fpthsout = self.client.gather(fpths)
            if output_mode == 'zarr':
                resize_zarr(fpthsout, totallen)
            del fpths
            return fpthsout
        else:
            return ''

    def _batch_read_write_netcdf(self, datatype, opts, converted_pth):
        """
        Take in list of xarray futures (output_arrs) and write them to netcdf files.  You'll get one .nc file per
        chunk which serves as a handy block size when using xarray open_mfdataset later (chunks will be one per nc file)

        Parameters
        ----------
        datatype: str, one of 'range_angle', 'attitude', 'navigation'
        opts: dict, nested dictionary containing settings and input/output arrays depending on datatype, see
                    self.batch_read
        converted_pth: str, path to the directory that will contain the written netcdf files

        Returns
        -------
        fpths: list, paths to all written netcdf files

        """
        output_pths = [converted_pth] * len(opts[datatype]['output_arrs'])
        output_fnames = [datatype + '.nc'] * len(opts[datatype]['output_arrs'])
        output_attributes = [opts[datatype]['final_attrs']] * len(opts[datatype]['output_arrs'])
        fname_idxs = [i for i in range(len(opts[datatype]['output_arrs']))]
        fpths = self.client.map(xarr_to_netcdf, opts[datatype]['output_arrs'], output_pths, output_fnames,
                                output_attributes, fname_idxs)
        return fpths

    def _batch_read_write_zarr(self, datatype, opts, converted_pth, secid=None):
        """
        Take in list of xarray futures (output_arrs) and write them to a single Zarr datastore.  Each array will become
        a Zarr array within the root Zarr group.  Zarr chunksize on read is determined by the chunks written, so the
        structure here is handy for generating identical, evenly spaced chunks (required by Zarr)

        Parameters
        ----------
        datatype: str, one of 'range_angle', 'attitude', 'navigation'
        opts: dict, nested dictionary containing settings and input/output arrays depending on datatype, see
                    self.batch_read
        converted_pth: str, path to the directory that will contain the written netcdf files

        Returns
        -------
        fpth: str, path to written zarr datastore/group.  I use the first element of the list of fpths as all returned
                   elements of fpths are identical.

        """
        if secid is None:
            output_pth = os.path.join(converted_pth, datatype + '.zarr')
        else:
            output_pth = os.path.join(converted_pth, datatype + '_' + secid + '.zarr')

        sync = DaskProcessSynchronizer(output_pth)
        # build out the instructions for each worker, start/end/xarray object
        data_locs = [[i * opts[datatype]['chunksize'], (i + 1) * opts[datatype]['chunksize']] for i in
                     range(len(opts[datatype]['output_arrs']))]
        fpths = [self.client.submit(my_xarr_to_zarr, opts[datatype]['output_arrs'][0], opts[datatype]['final_attrs'],
                                    output_pth, sync, data_locs[0], finalsize=len(data_locs) * data_locs[0][1])]
        wait(fpths[0])
        if len(opts[datatype]['output_arrs']) > 1:
            for i in range(len(opts[datatype]['output_arrs']) - 1):
                data_locs = [[i * opts[datatype]['chunksize'], (i + 1) * opts[datatype]['chunksize']] for i in
                             range(len(opts[datatype]['output_arrs']))]
                fpths.append(self.client.submit(my_xarr_to_zarr, opts[datatype]['output_arrs'][i + 1],
                                                opts[datatype]['final_attrs'], output_pth, sync, data_locs[i + 1]))
            wait(fpths[1:])
        fpth = fpths[0]  # Pick the first element, all are identical so it doesnt really matter
        return fpth

    def batch_read(self, output_mode='zarr'):
        """
        General converter for .all files leveraging xarray and dask.distributed
        See batch_read, same process but working on memory efficiency

        Parameters
        ----------
        output_mode: str, 'zarr' or 'netcdf' are the only currently supported modes, alters the output datastore

        Returns
        -------
        opts: dict, nested dictionary for each type (range_angle, attitude, navigation) that looks something like this:
              {'input_xarrs': None, 'chunksize': 5000, 'combine_attributes': True, output_arrs': None,
              'final_pths': list of path(s) to file(s), 'final_attrs': combined attributes for dataset}

        """
        print('Running Kongsberg .all converter...')
        starttime = perf_counter()

        if output_mode not in ['zarr', 'netcdf']:
            msg = 'Only zarr and netcdf modes are supported at this time: {}'.format(output_mode)
            raise NotImplementedError(msg)

        if self.client is None:
            self.client = dask_find_or_start_client()

        if self.client is not None:
            fils, converted_pth = self._batch_read_file_setup()
            webbrowser.open_new('http://localhost:8787/status')
            chnks_flat = self._batch_read_chunk_generation(fils)
            newrecfutures = self._batch_read_sequential_and_trim(chnks_flat)

            # xarrfutures is a list of futures representing xarray structures for each file chunk
            xarrfutures = self.client.map(_sequential_to_xarray, newrecfutures)
            del newrecfutures

            finalpths = {'range_angle': [], 'attitude': [], 'navigation': []}
            for datatype in ['range_angle', 'attitude', 'navigation']:
                opts = batch_read_configure_options()
                input_xarrs = self.client.map(_divide_xarray_futs, xarrfutures, [datatype] * len(xarrfutures))
                input_xarrs = self._batch_read_sort_futures_by_time(input_xarrs)
                if datatype in ['range_angle']:
                    finalattrs = self.client.gather(self.client.map(gather_dataset_attributes, input_xarrs))
                    opts['range_angle']['final_attrs'] = combine_xr_attributes(finalattrs)

                # rebalance to get equal chunksize in time dimension (sector/beams should be constant across)
                if datatype == 'range_angle' and len(input_xarrs) > 1:
                    sectors = self.client.gather(self.client.map(_return_xarray_sectors, input_xarrs))
                    totalsecs = sorted(np.unique([s for secs in sectors for s in secs]))
                    input_xarrs = self._batch_read_correct_block_boundaries(input_xarrs, totalsecs=totalsecs)

                opts[datatype]['output_arrs'], opts[datatype]['chunksize'], totallen = self._batch_read_merge_blocks(
                    input_xarrs, datatype, opts[datatype]['chunksize'])
                del input_xarrs
                finalpths[datatype].append(self._batch_read_write('zarr', datatype, opts, converted_pth, totallen))
                del opts

            endtime = perf_counter()
            print('Distributed conversion complete: {}s\n'.format(endtime - starttime))

            return finalpths
        return None

    def batch_read_by_sector(self, output_mode='zarr'):
        """
        General converter for .all files leveraging xarray and dask.distributed
        See batch_read, same process but working on memory efficiency

        Parameters
        ----------
        output_mode: str, 'zarr' or 'netcdf' are the only currently supported modes, alters the output datastore

        Returns
        -------
        opts: dict, nested dictionary for each type (range_angle, attitude, navigation) that looks something like this:
              {'input_xarrs': None, 'chunksize': 5000, 'combine_attributes': True, output_arrs': None,
              'final_pths': list of path(s) to file(s), 'final_attrs': combined attributes for dataset}

        """
        print('Running Kongsberg .all converter...')
        starttime = perf_counter()

        if output_mode not in ['zarr', 'netcdf']:
            msg = 'Only zarr and netcdf modes are supported at this time: {}'.format(output_mode)
            raise NotImplementedError(msg)

        if self.client is None:
            self.client = dask_find_or_start_client()

        if self.client is not None:
            fils, converted_pth = self._batch_read_file_setup()
            webbrowser.open_new('http://localhost:8787/status')
            chnks_flat = self._batch_read_chunk_generation(fils)
            newrecfutures = self._batch_read_sequential_and_trim(chnks_flat)

            # xarrfutures is a list of futures representing xarray structures for each file chunk
            xarrfutures = self.client.map(_sequential_to_xarray, newrecfutures)
            del newrecfutures

            finalpths = {'range_angle': [], 'attitude': [], 'navigation': []}
            for datatype in ['range_angle', 'attitude', 'navigation']:
                input_xarrs = self.client.map(_divide_xarray_futs, xarrfutures, [datatype] * len(xarrfutures))
                input_xarrs = self._batch_read_sort_futures_by_time(input_xarrs)
                if datatype == 'range_angle':
                    finalattrs = self.client.gather(self.client.map(gather_dataset_attributes, input_xarrs))
                    combattrs = combine_xr_attributes(finalattrs)
                    sectors = self.client.gather(self.client.map(_return_xarray_sectors, input_xarrs))
                    totalsecs = sorted(np.unique([s for secs in sectors for s in secs]))
                    for sec in totalsecs:
                        print('Operating on sector {}, s/n {}, freq {}'.format(sec.split('_')[1], sec.split('_')[0],
                                                                               sec.split('_')[2]))
                        input_xarrs_by_sec = self._batch_read_return_xarray_by_sector(input_xarrs, sec)
                        opts = batch_read_configure_options()
                        opts['range_angle']['final_attrs'] = combattrs
                        if len(input_xarrs_by_sec) > 1:
                            # rebalance to get equal chunksize in time dimension (sector/beams are constant across)
                            input_xarrs_by_sec = self._batch_read_correct_block_boundaries(input_xarrs_by_sec)
                        opts[datatype]['output_arrs'], opts[datatype]['chunksize'], totallen = self._batch_read_merge_blocks(input_xarrs_by_sec, datatype, opts[datatype]['chunksize'])
                        del input_xarrs_by_sec
                        finalpths[datatype].append(self._batch_read_write('zarr', datatype, opts, converted_pth, totallen, secid=sec))
                        del opts
                else:
                    opts = batch_read_configure_options()
                    opts[datatype]['output_arrs'], opts[datatype]['chunksize'], totallen = self._batch_read_merge_blocks(input_xarrs, datatype, opts[datatype]['chunksize'])
                    del input_xarrs
                    finalpths[datatype].append(self._batch_read_write('zarr', datatype, opts, converted_pth, totallen))
                    del opts

            endtime = perf_counter()
            print('Distributed conversion complete: {}s\n'.format(endtime - starttime))

            return finalpths
        return None

    def build_offsets(self):
        """
        Form sorteddict for unique entries in installation parameters across all lines, retaining the xyzrph for each
        transducer/receiver.  key values depend on type of sonar, see self.ky_data73_sonar_translator

        Modifies the xyzrph attribute with timestamp dictionary of entries
        """
        self.xyzrph = {}

        settdict = {}
        setts = [x for x in self.raw_rangeangle[0].attrs if x[0:8] == 'settings']
        for sett in setts:
            settdict[sett.split('_')[1]] = json.loads(self.raw_rangeangle[0].attrs[sett])

        print('Found {} total Installation Parameters entr(y)s'.format(len(settdict)))
        if len(settdict) > 0:
            snrmodels = np.unique([settdict[x]['sonar_model_number'] for x in settdict])
            if len(snrmodels) > 1:
                raise NotImplementedError('ERROR: Found multiple sonars types in data provided')
            if snrmodels[0] not in self.ky_data73_sonar_translator:
                raise NotImplementedError('ERROR: Sonar model not understood "{}"'.format(snrmodels[0]))

            mintime = min(list(settdict.keys()))
            minactual = self.raw_rangeangle[0].time.min().compute()
            if float(mintime) > float(minactual):
                print('Installation Parameters minimum time: {}'.format(mintime))
                print('Actual data minimum time: {}'.format(float(minactual)))
                print('First Installation Parameters does not cover the start of the dataset.' +
                      '  Extending from nearest entry...')
                settdict[str(int(minactual))] = settdict.pop(mintime)

            # translate over the offsets/angles for the transducers following the sonar_translator scheme
            self.sonartype = snrmodels[0]
            for tme in settdict:
                self.xyzrph[tme] = {}
                for val in [v for v in self.ky_data73_sonar_translator[self.sonartype] if
                            v is not None]:  # tx, rx, etc.
                    ky = self.ky_data73_sonar_translator[self.sonartype].index(val)  # 0, 1, 2, etc
                    if val == 'txrx':
                        # for right now, if you have a sonar like the 2040c where rx and tx are basically in the same
                        #   physical container (with the same offsets), just make the tx and rx entries the same
                        self.xyzrph[tme]['tx_x'] = settdict[tme]['transducer_{}_along_location'.format(ky)]
                        self.xyzrph[tme]['tx_y'] = settdict[tme]['transducer_{}_athwart_location'.format(ky)]
                        self.xyzrph[tme]['tx_z'] = settdict[tme]['transducer_{}_vertical_location'.format(ky)]
                        self.xyzrph[tme]['tx_r'] = settdict[tme]['transducer_{}_roll_angle'.format(ky)]
                        self.xyzrph[tme]['tx_p'] = settdict[tme]['transducer_{}_pitch_angle'.format(ky)]
                        self.xyzrph[tme]['tx_h'] = settdict[tme]['transducer_{}_heading_angle'.format(ky)]
                        self.xyzrph[tme]['rx_x'] = settdict[tme]['transducer_{}_along_location'.format(ky)]
                        self.xyzrph[tme]['rx_y'] = settdict[tme]['transducer_{}_athwart_location'.format(ky)]
                        self.xyzrph[tme]['rx_z'] = settdict[tme]['transducer_{}_vertical_location'.format(ky)]
                        self.xyzrph[tme]['rx_r'] = settdict[tme]['transducer_{}_roll_angle'.format(ky)]
                        self.xyzrph[tme]['rx_p'] = settdict[tme]['transducer_{}_pitch_angle'.format(ky)]
                        self.xyzrph[tme]['rx_h'] = settdict[tme]['transducer_{}_heading_angle'.format(ky)]
                    else:
                        self.xyzrph[tme][val + '_x'] = settdict[tme]['transducer_{}_along_location'.format(ky)]
                        self.xyzrph[tme][val + '_y'] = settdict[tme]['transducer_{}_athwart_location'.format(ky)]
                        self.xyzrph[tme][val + '_z'] = settdict[tme]['transducer_{}_vertical_location'.format(ky)]
                        self.xyzrph[tme][val + '_r'] = settdict[tme]['transducer_{}_roll_angle'.format(ky)]
                        self.xyzrph[tme][val + '_p'] = settdict[tme]['transducer_{}_pitch_angle'.format(ky)]
                        self.xyzrph[tme][val + '_h'] = settdict[tme]['transducer_{}_heading_angle'.format(ky)]

                # additional offsets based on sector
                if self.sonartype in self.ky_data73_install_modifier:
                    for val in [v for v in self.ky_data73_install_modifier[self.sonartype] if v is not None]:
                        for sec in self.ky_data73_install_modifier[self.sonartype][val]:
                            self.xyzrph[tme][val + '_x_' + sec] = \
                                self.ky_data73_install_modifier[self.sonartype][val][sec]['x']
                            self.xyzrph[tme][val + '_y_' + sec] = \
                                self.ky_data73_install_modifier[self.sonartype][val][sec]['y']
                            self.xyzrph[tme][val + '_z_' + sec] = \
                                self.ky_data73_install_modifier[self.sonartype][val][sec]['z']

                # translate over the positioning sensor stuff using the installation parameters active identifiers
                pos_ident = settdict[tme]['active_position_system_number']  # 'position_1'
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

                # include waterline if it exists
                if 'waterline_vertical_location' in settdict[tme]:
                    self.xyzrph[tme]['waterline'] = settdict[tme]['waterline_vertical_location']
                else:
                    print('No waterline value found')

                # generate dict of ordereddicts for fast searching
                newdict = {}
                for ky in self.xyzrph:
                    for stmp in self.xyzrph[ky].keys():
                        if stmp not in newdict:
                            newdict[stmp] = SortedDict()
                        newdict[stmp][ky] = self.xyzrph[ky][stmp]
                self.xyzrph = SortedDict(newdict)
                print('Constructed offsets successfully')

    def _get_nth_chunk_indices(self, chunks, idx):
        """
        Take the output of Xarray DataArray chunks and produce start/end indices for chunk idx

        Parameters
        ----------
        chunks: tuple, where each element is the length of the chunk ex: ((5000, 5000, 323),)
        idx: int, the index of the chunk you want the indices for

        Returns
        -------
        start_idx, end_idx: int, indices for start/end of chunk
        """

        if len(chunks) > 1:
            raise NotImplementedError('Only supporting 1d chunks currently')
        chunks = chunks[0]
        start_idx = np.sum([i for i in chunks if chunks.index(i) < idx])
        end_idx = chunks[idx] + start_idx
        return start_idx, end_idx

    def _interp_nan_nearest(self, arr):
        """
        Fill nan values according to nearest.  Helper function to make this happen given apparent limitations in
        existing xarray/scipy methods.  See below

        Parameters
        ----------
        arr: Xarray DataArray object, should contain nan values to interpolate

        Returns
        -------
        arr: Xarray DataArray object with nan values interpolated according to nearest val
        """
        arr = arr.where(arr != 0)
        nanvals = np.isnan(arr)
        uniquevals = np.unique(arr[~nanvals])
        if np.any(nanvals):
            if len(uniquevals) == 1:
                print('_interp_nan_nearest: ({}) Applying simple fillna interp' +
                      ' for array with one unique entry'.format(arr.name))
                arr = arr.fillna(uniquevals[0])
            else:
                print('_interp_nan_nearest: ({}) Propagating previous value forward for array' +
                      ' with multiple unique entries'.format(arr.name))
                arr = arr.ffill('time')
                new_nanvals = np.isnan(arr)
                if np.any(new_nanvals):
                    # if you get here, you must have an entire chunk without a non-nan entry.  Interp stuff won't work
                    #   unless theres at least one good entry in the chunk.  Use last value from previous chunk
                    print('_interp_nan_nearest: ({}) Found isolated chunks without values...'.format(arr.name))
                    for ct, chunk in enumerate(arr.chunks):
                        start_idx, end_idx = self._get_nth_chunk_indices(arr.chunks, ct)
                        if np.any(new_nanvals[start_idx:end_idx]):
                            if ct == 0:
                                print('_interp_nan_nearest: ({}) Filling chunk 0 with next values'.format(arr.name))
                                goodvalsearch = np.argwhere(nanvals[start_idx:end_idx + 1] is False)[-1][0]
                            else:
                                print(
                                    '_interp_nan_nearest: ({}) Filling chunk {}} with previous values'.format(arr.name,
                                                                                                              ct))
                                goodvalsearch = np.argwhere(nanvals[start_idx - 1:end_idx] is False)[-1][0]
                            lastgoodval = arr[goodvalsearch]
                            arr[start_idx:end_idx] = arr[start_idx:end_idx].fillna(lastgoodval)
        return arr

    def is_dual_head(self):
        """
        Use the xyzrph keys to determine if sonar is dual head.  Port/Starboard identifiers will exist if dual.

        Returns
        -------
        True if dual head, False if not
        """
        if self.xyzrph is None:
            self.build_offsets()
        if ('tx_port_x' in list(self.xyzrph.keys())) or ('rx_port_x' in list(self.xyzrph.keys())):
            return True
        else:
            return False

    def return_xyzrph_sorted_timestamps(self, ky):
        """
        Takes in key name and outputs a list of sorted timestamps that are valid for that key.

        Parameters
        ----------
        ky: str, key name that you want the timestamps from

        Returns
        -------
        tstmps: list, sorted timestamps of type str, in increasing order
        """
        tstmps = list(self.xyzrph[ky].keys())
        tstmps.sort()
        return tstmps

    def return_tx_xyzrph(self, time_idx):
        """
        Using the constructed xyzrph attribute (see build_offsets) and a given timestamp, return the
        transmitter offsets and angles nearest in time to the timestamp

        Parameters
        ----------
        time_idx = timestamp (accepts int/str/float)

        Returns
        -------
        dict, key = closest timestamp and values = mounting angle/offsets for receiver

        """
        if self.xyzrph is None:
            self.build_offsets()
        if self.sonartype in ['em2040', 'em122', 'em710', 'em2045', 'em2040p']:
            corr_timestmp = str(int(_closest_prior_key_value(list(self.xyzrph['tx_r'].keys()), time_idx)))
            return {corr_timestmp: {'tx_roll': float(self.xyzrph['tx_r'][corr_timestmp]),
                                    'tx_pitch': float(self.xyzrph['tx_p'][corr_timestmp]),
                                    'tx_heading': float(self.xyzrph['tx_h'][corr_timestmp]),
                                    'tx_x': float(self.xyzrph['tx_x'][corr_timestmp]),
                                    'tx_y': float(self.xyzrph['tx_y'][corr_timestmp]),
                                    'tx_z': float(self.xyzrph['tx_z'][corr_timestmp])}}

        elif self.sonartype in ['em2040_dual_rx', 'em2040_dual_tx']:
            corr_timestmp = str(int(_closest_prior_key_value(list(self.xyzrph['tx_port_r'].keys()), time_idx)))
            return {corr_timestmp: {'tx_port_roll': float(self.xyzrph['tx_port_r'][corr_timestmp]),
                                    'tx_port_pitch': float(self.xyzrph['tx_port_p'][corr_timestmp]),
                                    'tx_port_heading': float(self.xyzrph['tx_port_h'][corr_timestmp]),
                                    'tx_port_x': float(self.xyzrph['tx_port_x'][corr_timestmp]),
                                    'tx_port_y': float(self.xyzrph['tx_port_y'][corr_timestmp]),
                                    'tx_port_z': float(self.xyzrph['tx_port_z'][corr_timestmp]),
                                    'tx_stbd_roll': float(self.xyzrph['tx_stbd_r'][corr_timestmp]),
                                    'tx_stbd_pitch': float(self.xyzrph['tx_stbd_p'][corr_timestmp]),
                                    'tx_stbd_heading': float(self.xyzrph['tx_stbd_h'][corr_timestmp]),
                                    'tx_stbd_x': float(self.xyzrph['tx_stbd_x'][corr_timestmp]),
                                    'tx_stbd_y': float(self.xyzrph['tx_stbd_y'][corr_timestmp]),
                                    'tx_stbd_z': float(self.xyzrph['tx_stbd_z'][corr_timestmp])
                                    }}
        else:
            raise NotImplementedError('Sonartype not supported: {}'.format(self.sonartype))

    def return_rx_xyzrph(self, time_idx):
        """
        Using the constructed xyzrph attribute (see build_offsets) and a given timestamp, return the
        receiver offsets and angles nearest in time to the timestamp

        Parameters
        ----------
        time_idx = timestamp (accepts int/str/float)

        Returns
        -------
        dict, key = closest timestamp and values = mounting angle/offsets for receiver

        """
        if self.xyzrph is None:
            self.build_offsets()
        if self.sonartype in ['em2040', 'em122', 'em710', 'em2045', 'em2040p']:
            corr_timestmp = str(int(_closest_prior_key_value(list(self.xyzrph['rx_r'].keys()), time_idx)))
            return {corr_timestmp: {'rx_roll': float(self.xyzrph['rx_r'][corr_timestmp]),
                                    'rx_pitch': float(self.xyzrph['rx_p'][corr_timestmp]),
                                    'rx_heading': float(self.xyzrph['rx_h'][corr_timestmp]),
                                    'rx_x': float(self.xyzrph['rx_x'][corr_timestmp]),
                                    'rx_y': float(self.xyzrph['rx_y'][corr_timestmp]),
                                    'rx_z': float(self.xyzrph['rx_z'][corr_timestmp])}}

        elif self.sonartype in ['em2040_dual_rx', 'em2040_dual_tx']:
            corr_timestmp = str(int(_closest_prior_key_value(list(self.xyzrph['rx_port_r'].keys()), time_idx)))
            return {corr_timestmp: {'rx_port_roll': float(self.xyzrph['rx_port_r'][corr_timestmp]),
                                    'rx_port_pitch': float(self.xyzrph['rx_port_p'][corr_timestmp]),
                                    'rx_port_heading': float(self.xyzrph['rx_port_h'][corr_timestmp]),
                                    'rx_port_x': float(self.xyzrph['rx_port_x'][corr_timestmp]),
                                    'rx_port_y': float(self.xyzrph['rx_port_y'][corr_timestmp]),
                                    'rx_port_z': float(self.xyzrph['rx_port_z'][corr_timestmp]),
                                    'rx_stbd_roll': float(self.xyzrph['rx_stbd_r'][corr_timestmp]),
                                    'rx_stbd_pitch': float(self.xyzrph['rx_stbd_p'][corr_timestmp]),
                                    'rx_stbd_heading': float(self.xyzrph['rx_stbd_h'][corr_timestmp]),
                                    'rx_stbd_x': float(self.xyzrph['rx_stbd_x'][corr_timestmp]),
                                    'rx_stbd_y': float(self.xyzrph['rx_stbd_y'][corr_timestmp]),
                                    'rx_stbd_z': float(self.xyzrph['rx_stbd_z'][corr_timestmp])
                                    }}
        else:
            raise NotImplementedError('Sonartype not supported: {}'.format(self.sonartype))

    def return_nearest_soundspeed_profile(self, time_idx):
        """
        Using the settings_xxxxx attribute in the xarray dataset and a given timestamp, return the waterline
        offset (relative to the tx) nearest in time to the timestamp.

        Parameters
        ----------
        time_idx = timestamp (accepts int/str/float)

        Returns
        -------
        dict, key = closest timestamp and value = waterline offset

        """
        profs = [x for x in self.raw_rangeangle[0].attrs.keys() if x[0:7] == 'profile']
        if len(profs) == 0:
            raise ValueError('No settings attributes found, possibly no install params in .all files')

        prof_tims = [float(x.split('_')[1]) for x in profs]
        closest_tim = str(int(_closest_key_value(prof_tims, time_idx)))
        return self.raw_rangeangle[0].attrs['profile_{}'.format(closest_tim)]

    def return_ping_counters_at_time(self, tme):
        """
        Accepts times as float or a numpy array of times

        To rebuild the full ping at a specific time, you need to get the ping counter(s) at that time.  EM2040c
        have multiple pings at a specific time, so this will return a list of counters that is usually only one
        element long.

        Parameters
        ----------
        tme: float or numpy array, time to find ping counters for

        Returns
        -------
        cntrs: list, list of ints for ping counter numbers at that time

        """
        cntrs = None
        for ra in self.raw_rangeangle:
            overlap = np.intersect1d(ra.time, tme)
            if np.any(overlap):
                if cntrs is None:
                    cntrs = ra.sel(time=overlap).counter.values
                else:
                    cntrs = np.concatenate([cntrs, ra.sel(time=overlap).counter.values])
        return np.unique(cntrs)

    def return_active_sectors_for_ping_counter(self, rangeangle_idx, ping_counters):
        """
        Take in the ping counter number(s) and return all the times the ping counter exists in the given rangeangle

        Parameters
        ----------
        rangeangle_idx: int, index to select the range angle dataset
        ping_counters: int or numpy array, ping counter number

        Returns
        -------
        ans: numpy array of size equal to ping_counters, with elements filled with ping time if that ping counter is in
                       the given range angle dataset, ex: [0, 0, 1531318885.14])

        """
        ra = self.raw_rangeangle[rangeangle_idx]
        ans = np.zeros_like(ping_counters, dtype=np.float64)

        ra_cntr_msk = np.isin(ra.counter, ping_counters)
        ping_cntr_msk = np.isin(ping_counters, ra.counter[ra_cntr_msk].values)

        counter_time = ra.time[ra_cntr_msk].values
        ans[ping_cntr_msk] = counter_time
        return ans

    def return_all_profiles(self):
        """
        Return dict of attribute_name/data for each sv profile in the range_angle dataset

        attribute name is always 'profile_timestamp' format, ex: 'profile_1503411780'

        Returns
        -------
        dict, dictionary of attribute_name/data for each sv profile

        """
        prof_keys = [x for x in self.raw_rangeangle[0].attrs.keys() if x[0:7] == 'profile']
        if prof_keys:
            return {p: self.raw_rangeangle[0].attrs[p] for p in prof_keys}
        else:
            return {}

    def return_waterline(self, time_idx):
        """
        Using the settings_xxxxx attribute in the xarray dataset and a given timestamp, return the waterline
        offset (relative to the tx) nearest in time to the timestamp.

        Parameters
        ----------
        time_idx = timestamp (accepts int/str/float)

        Returns
        -------
        dict, key = closest timestamp and value = waterline offset

        """
        settrecs = [x for x in self.raw_rangeangle[0].attrs.keys() if x[0:8] == 'settings']
        if len(settrecs) == 0:
            raise ValueError('No settings attributes found, possibly no installation parameters in source files')

        sett_tims = [float(x.split('_')[1]) for x in settrecs]
        closest_tim = str(int(_closest_prior_key_value(sett_tims, time_idx)))
        return float(json.loads(self.raw_rangeangle[0].attrs['settings_' + closest_tim])['waterline_vertical_location'])

    def return_xyz_prefixes_for_sectors(self):
        """
        self.raw_rangeangle contains Datasets broken up by sector.  This method will return the prefixes you need to get
        the offsets/angles from self.xyzrph depending on model/dual-head

        Returns
        -------
        lever_prefix: List, list of two element lists containing the prefixes needed for tx/rx offsets and angles

        """
        if 'tx_r' in self.xyzrph:
            leverarms = [['tx', 'rx']]
        elif 'tx_port_r' in self.xyzrph:
            leverarms = [['tx_port', 'rx_port'], ['tx_stbd', 'rx_stbd']]
        else:
            raise NotImplementedError('Not supporting this sonartype yet.')

        lever_prefix = []
        for ra in self.raw_rangeangle:
            # dual head logic
            if len(leverarms) > 1:
                if ra.sector_identifier[0:3] == str(ra.system_serial_number[0]):
                    lever_prefix.append(leverarms[0])
                elif ra.sector_identifier[0:3] == str(ra.secondary_system_serial_number[0]):
                    lever_prefix.append(leverarms[1])
                else:
                    raise NotImplementedError('Found serial number attribute not included in sector')
            else:
                lever_prefix.append(leverarms[0])
        return lever_prefix

    def select_array_from_rangeangle(self, var, sector):
        """
        Given variable name and sectors, return DataArray specified by var reduced to dimensions specified by sectors.
        Using the ntx array as a key (identifies sectors/freq that are pinging at that time) returned array will
           have NaN for all unused sector/freq combinations

        Parameters
        ----------
        var: str, variable name to identify xarray DataArray to return
        sector: str, string name for sectors to subset DataArray by

        Returns
        -------
        DataArray object

        """
        for ra in self.raw_rangeangle:
            if ra.sector_identifier == sector:
                sec_dat = ra[var].where(ra.ntx > 0)
                if sec_dat.ndim == 2:
                    # for (time, beam) dimensional arrays, also replace 999.0 in the beam dimension with nan
                    #    to make downstream calculations a bit easier (see pad_to_dense)
                    return sec_dat.where(sec_dat != 999.0)
                else:
                    return sec_dat
        return None

    def return_range_angle_by_sector(self, secid):
        """
        Given sector identifier (secid), return the correct range_angle Dataset from the list

        Parameters
        ----------
        secid: str, name of the sector you want

        Returns
        -------
        xarray Dataset, Dataset for the right sector

        """
        for ra in self.raw_rangeangle:
            if ra.sector_identifier == secid:
                return ra
        return None

    def return_sector_time_indexed_array(self):
        """
        Most of the processing involves matching static, timestamped offsets or angles to time series data.  Given that
        we might have a list of sectors and a list of timestamped offsets, need to iterate through all of this in each
        processing loop.  Sectors/timestamps length should be minimal, so we just loop in python.

        Returns
        -------
        resulting_sectors: list of lists of xarray DataArrays representing the times associated with each sector/
                        timestamped installation offset as well as sector identifiers.

                Example below for 2 sectors and one installation parameter timestamped entry
        [
        [[<xarray.DataArray 'time' (time: 10)> array([True, True, True, True, True, True, True, True, True, True])
           Coordinates:
             * time     (time) float64 1.496e+09 1.496e+09 ... 1.496e+09 1.496e+09], '1495563079', ['tx', 'rx']],
        [[<xarray.DataArray 'time' (time: 10)> array([True, True, True, True, True, True, True, True, True, True])
           Coordinates:
             * time     (time) float64 1.496e+09 1.496e+09 ... 1.496e+09 1.496e+09], '1495563079', ['tx', 'rx']],
        ]

        """
        resulting_sectors = []
        prefixes = self.return_xyz_prefixes_for_sectors()
        for cnt, ra in enumerate(self.raw_rangeangle):
            txrx = prefixes[cnt]
            tstmps = self.return_xyzrph_sorted_timestamps(txrx[0] + '_x')
            resulting_tstmps = []
            for tstmp in tstmps:

                # fudge factor for how the install param record is sometime included up to a couple seconds after the
                #    start of the file
                mintime = float(ra.time.min())
                newtstmp = tstmp
                if abs(float(tstmp) - mintime) <= 2.0:
                    print('adjusting the install param record from {} to {}'.format(tstmp, mintime))
                    newtstmp = mintime

                # timestamps for range_angle records that are at or past the install param record we are using
                try:
                    # for when you have multiple timestamps in the installation parameters record
                    tx_idx = np.logical_and(ra.time >= float(newtstmp), ra.time < float(tstmps[tstmps.index(tstmp) + 1]))
                except IndexError:
                    tx_idx = ra.time >= float(newtstmp)
                resulting_tstmps.append([tx_idx, tstmp, txrx])
            resulting_sectors.append(resulting_tstmps)
        return resulting_sectors

    def get_minmax_extents(self):
        """
        Build dataset geographic extents
        """
        maxlat = self.raw_nav.latitude.max().compute()
        maxlon = self.raw_nav.longitude.max().compute()
        minlat = self.raw_nav.latitude.min().compute()
        minlon = self.raw_nav.longitude.min().compute()
        print('Max Lat/Lon: {}/{}'.format(maxlat, maxlon))
        print('Min Lat/Lon: {}/{}'.format(minlat, minlon))
        self.extents = [maxlat, maxlon, minlat, minlon]

    def show_description(self):
        """
        Display xarray Dataset description
        """
        print(self.raw_rangeangle[0].info)

    def generate_plots(self):
        """
        Generate some example plots showing the abilities of the xarray plotting engine
        - plot detection info for beams/sector/times
        - plot roll/pitch/heave on 3 row plot
        """
        self.raw_rangeangle.detectioninfo.plot(x='beam', y='time', col='sector', col_wrap=3)

        fig, axes = plt.subplots(nrows=3)
        self.raw_rangeangle['roll'].plot(ax=axes[0])
        self.raw_rangeangle['pitch'].plot(ax=axes[1])
        self.raw_rangeangle['heading'].plot(ax=axes[2])


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
        max_chunks = int(np.floor(min_chunks))
        finalchunksize = int(minchunksize + (((min_chunks % 1) * minchunksize) / max_chunks))
    else:
        # Need a higher chunksize to get less than max_chunks chunks
        # Take chunks over max_chunks and increase chunksize to get to max_chunks
        overflowchunks = min_chunks - max_chunks
        finalchunksize = int(minchunksize + ((overflowchunks * minchunksize) / max_chunks))

    print('{}: Using {} chunks of size {}'.format(fil, max_chunks, finalchunksize))
    return finalchunksize


def return_chunked_fil(fil, startoffset=0, chunksize=20 * 1024 * 1024):
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
    midchunks = [(t * chunksize + chunksize - startoffset) for t in range(int(filesize / chunksize))]

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
