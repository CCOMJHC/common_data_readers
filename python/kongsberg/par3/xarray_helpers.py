import os
import numpy as np
import json
import xarray as xr
from xarray.core.combine import _infer_concat_order_from_positions, _nested_combine
import zarr


def my_open_mfdataset(paths, chnks=None, concat_dim='time', compat='no_conflicts', data_vars='all',
                      coords='different', join='outer'):
    """
    Trying to address the limitations of the existing xr.open_mfdataset function.  This is my modification using
    the existing function and tweaking to resolve the issues i've found.

    (see https://github.com/pydata/xarray/blob/master/xarray/backends/api.py)

    Current issues with open_mfdataset (1/8/2020):
    1. open_mfdataset only uses the attrs from the first nc file
    2. open_mfdataset will not run with parallel=True or with the distributed.LocalCluster running
    3. open_mfdataset infers time order from position.  (I could just sort outside of the function, but i kinda
         like it this way anyway.  Also a re-indexing would probably resolve this.)

    Only resolved item=1 so far.  See https://github.com/pydata/xarray/issues/3684

    Returns
    -------
    combined: Xarray Dataset - with attributes, variables, dimensions of combined netCDF files.  Returns dask
            arrays, compute to access local numpy array.

    """
    # ensure file paths are valid
    pth_chk = np.all([os.path.exists(x) for x in paths])
    if not pth_chk:
        raise ValueError('Check paths supplied to function.  Some/all files do not exist.')

    # sort by filename index, e.g. rangeangle_0.nc, rangeangle_1.nc, rangeangle_2.nc, etc.
    idxs = [int(os.path.splitext(os.path.split(x)[1])[0].split('_')[1]) for x in paths]
    sortorder = sorted(range(len(idxs)), key=lambda k: idxs[k])

    # sort_paths are the paths in sorted order by the filename index
    sort_paths = [paths[p] for p in sortorder]

    # build out the arugments for the nested combine
    if isinstance(concat_dim, (str, xr.DataArray)) or concat_dim is None:
        concat_dim = [concat_dim]
    combined_ids_paths = _infer_concat_order_from_positions(sort_paths)
    ids, paths = (list(combined_ids_paths.keys()), list(combined_ids_paths.values()))
    if chnks is None:
        chnks = {}

    datasets = [xr.open_dataset(p, engine='netcdf4', chunks=chnks, lock=None, autoclose=None) for p in paths]

    combined = _nested_combine(datasets, concat_dims=concat_dim, compat=compat, data_vars=data_vars,
                               coords=coords, ids=ids, join=join)
    combined.attrs = combine_xr_attributes(datasets)
    return combined


def xarr_to_netcdf(xarr, pth, fname, attrs, idx=None):
    """
    Takes in an xarray Dataset and pushes it to netcdf.
    For use with the output from combine_xarrs and/or _sequential_to_xarray

    Returns
    -------
    finalpth: str - path to the netcdf file

    """
    if idx is not None:
        finalpth = os.path.join(pth, os.path.splitext(fname)[0] + '_{}.nc'.format(idx))
    else:
        finalpth = os.path.join(pth, fname)

    if attrs is not None:
        xarr.attrs = attrs

    xarr.to_netcdf(path=finalpth, format='NETCDF4', engine='netcdf4')
    return finalpth


def xarr_to_zarr(xarr, attrs, outputpth, sync):
    """
    Takes in an xarray Dataset and pushes it to zarr store.

    Must be run once to generate new store.
    Successive runs append, see mode flag

    Returns
    -------
    finalpth: str - path to the zarr group

    """
    # grpname = str(datetime.now().strftime('%H%M%S%f'))
    if attrs is not None:
        xarr.attrs = attrs

    if not os.path.exists(outputpth):
        xarr.to_zarr(outputpth, mode='w-', compute=False)
    else:
        xarr.to_zarr(outputpth, mode='a', synchronizer=sync, compute=False, append_dim='time')

    return outputpth


def _my_xarr_to_zarr_build_arraydimensions(xarr):
    """
    Build out dimensions/shape of arrays in xarray into a dict so that we can use it with the zarr writer.

    Parameters
    ----------
    xarr: xarray Dataset, one chunk of the final range_angle/attitude/navigation xarray Dataset we are writing

    Returns
    -------
    dims_of_arrays: dict, where keys are array names and values list of dims/shape.  Example:
                    'beampointingangle': [['time', 'sector', 'beam'], (5000, 3, 400)]

    """
    dims_of_arrays = {}
    arrays_in_xarr = list(xarr.variables.keys())
    for arr in arrays_in_xarr:
        dims_of_arrays[arr] = [xarr[arr].dims, xarr[arr].shape]
    return dims_of_arrays


def _my_xarr_to_zarr_writeattributes(rootgroup, attrs):
    """
    Take the attributes generated with combine_xr_attributes and write them to the final datastore

    Parameters
    ----------
    rootgroup: zarr group, zarr datastore group for one of range_angle/attitude/navigation
    attrs: dict, dictionary of combined attributes from xarray datasets, None if no attributes exist

    """
    if attrs is not None:
        for att in attrs:
            if att not in rootgroup.attrs:
                # ndarray is not json serializable
                if isinstance(attrs[att], np.ndarray):
                    attrs[att] = attrs[att].tolist()
                rootgroup.attrs[att] = attrs[att]


def resize_zarr(zarrpth, finaltimelength):
    """
    Takes in the path to a zarr group and resizes the time dimension according to the provided finaltimelength

    Parameters
    ----------
    zarrpth: str, path to a zarr group on the filesystem
    finaltimelength: int, new length for the time dimension

    """
    # the last write will often be less than the block size.  This is allowed in the zarr store, but we
    #    need to correct the index for it.
    rootgroup = zarr.open(zarrpth, mode='r+')
    for var in rootgroup.arrays():
        if var[0] not in ['beam', 'sector']:
            varname = var[0]
            dims = rootgroup[varname].attrs['_ARRAY_DIMENSIONS']
            time_index = dims.index('time')
            new_shape = list(rootgroup[varname].shape)
            new_shape[time_index] = finaltimelength
            rootgroup[varname].resize(tuple(new_shape))


def combine_xr_attributes(datasets):
    """
    xarray open_mfdataset only retains the attributes of the first dataset.  We store profiles and installation
    parameters in datasets as they arise.  We need to combine the attributes across all datasets for our final
    dataset.

    Parameters
    ----------
    datasets: list of xarray.Datasets representing range_angle for our workflow.  Can be any dataset object though.  We
              are just storing attributes in the range_angle one so far.

    Returns
    -------
    finaldict: dict, contains all unique attributes across all dataset, will append unique prim/secondary serial
        numbers and ignore duplicate settings entries
    """
    finaldict = {}

    buffered_settings = ''
    fnames = []
    survey_nums = []

    if type(datasets) != list:
        datasets = [datasets]

    try:
        all_attrs = [datasets[x].attrs for x in range(len(datasets))]
    except AttributeError:
        all_attrs = datasets

    for d in all_attrs:
        for k, v in d.items():
            # settings gets special treatment for a few reasons...
            if k[0:8] == 'settings':
                vals = json.loads(v)  # stored as a json string for serialization reasons
                fname = vals.pop('raw_file_name')
                if fname not in fnames:
                    # keep .all file names for their own attribute
                    fnames.append(fname)
                sname = vals.pop('survey_identifier')
                if sname not in survey_nums:
                    # keep survey identifiers for their own attribute
                    survey_nums.append(sname)
                vals = json.dumps(vals)

                # This is for the duplicate entries, just ignore these
                if vals == buffered_settings:
                    pass
                # this is for the first settings entry
                elif not buffered_settings:
                    buffered_settings = vals
                    finaldict[k] = vals
                # all unique entries after the first are saved
                else:
                    finaldict[k] = vals

            # save all unique serial numbers
            elif k in ['system_serial_number', 'secondary_system_serial_number'] and k in list(finaldict.keys()):
                if finaldict[k] != v:
                    finaldict[k] = np.array(finaldict[k])
                    finaldict[k] = np.append(finaldict[k], v)
            elif k not in finaldict:
                finaldict[k] = v

    if fnames:
        finaldict['kongsberg_files'] = list(np.unique(sorted(fnames)))
    if survey_nums:
        finaldict['survey_number'] = list(np.unique(survey_nums))
    return finaldict


def my_xarr_to_zarr(xarr, attrs, outputpth, sync, dataloc, finalsize=None):
    """
    I've been unable to get the zarr append/write functionality to work with the dask distributed cluster.  Even when
    using dask's own distributed lock, I've found that the processes are stepping on each other and each array is not
    written in it's entirety.

    The solution I have here ignores the zarr append method and manually specifies indexes for each worker to write to
    (dataloc).  I believe with this method, you probably don't even need the sync, but I leave it in here in case it
    comes into play when writing attributes/metadata.

    Parameters
    ----------
    xarr: xarray Dataset, one chunk of the final range_angle/attitude/navigation xarray Dataset we are writing
    attrs: dict, dictionary of combined attributes from xarray datasets, None if no attributes exist
    outputpth: str, path to zarr group to either be created or append to
    sync: DaskProcessSynchronizer, dask distributed lock for parallel read/writes
    dataloc: list, list of start/end time indexes, ex: [100,600] for reading from the 100th time to the 600th time in
                   this chunk
    finalsize: int, if given, resize the zarr group time dimension to this value, if None then do no resizing

    Returns
    -------
    outputpth: pth, path to the final zarr group

    """
    dims_of_arrays = _my_xarr_to_zarr_build_arraydimensions(xarr)

    # mode 'a' means read/write, create if doesnt exist
    rootgroup = zarr.open(outputpth, mode='a', synchronizer=sync)
    existing_arrs = [t for t in rootgroup.array_keys()]

    _my_xarr_to_zarr_writeattributes(rootgroup, attrs)

    # var here will represent one of the array names, 'beampointingangle', 'time', 'soundspeed', etc.
    for var in dims_of_arrays:
        data_loc_copy = dataloc.copy()
        # these do not need appending, just ensure they maintain uniques
        if var in ['sector', 'beam']:
            if var in existing_arrs:
                if not np.array_equal(xarr[var].values, rootgroup[var]):
                    raise ValueError(
                        'Found inconsistent ' + var + ' dimension: ' + xarr[var].values + ' and ' + rootgroup[var])
            else:
                rootgroup[var] = xarr[var].values
        else:
            # want to get the length of the time dimension, so you know which dim to append to
            timlength = len(xarr[var]['time'])
            timaxis = xarr[var].shape.index(timlength)
            # shape is extended on append.  chunks will always be equal to shape, as each run of this function will be
            #     done on one chunk of data by one worker

            if var in existing_arrs:
                # array to be appended
                newarr = zarr.array(xarr[var].values, shape=dims_of_arrays[var][1], chunks=dims_of_arrays[var][1])

                # the last write will often be less than the block size.  This is allowed in the zarr store, but we
                #    need to correct the index for it.
                if timlength != data_loc_copy[1] - data_loc_copy[0]:
                    data_loc_copy[1] = data_loc_copy[0] + timlength

                # location for new data, assume constant chunksize (as we are doing this outside of this function)
                chunk_time_range = slice(data_loc_copy[0], data_loc_copy[1])
                chunk_idx = tuple(
                    chunk_time_range if dims_of_arrays[var][1].index(i) == timaxis else slice(0, i) for i in
                    dims_of_arrays[var][1])
                rootgroup[var][chunk_idx] = newarr
            else:
                startingshp = tuple(
                    finalsize if dims_of_arrays[var][1].index(x) == timaxis else x for x in dims_of_arrays[var][1])

                newarr = rootgroup.create_dataset(var, shape=dims_of_arrays[var][1], chunks=dims_of_arrays[var][1],
                                                  dtype=xarr[var].dtype, synchronizer=sync, fill_value=None)
                newarr[:] = xarr[var].values
                newarr.resize(startingshp)

        # _ARRAY_DIMENSIONS is used by xarray for connecting dimensions with zarr arrays
        rootgroup[var].attrs['_ARRAY_DIMENSIONS'] = dims_of_arrays[var][0]
    return outputpth


def _interp_across_chunks_xarrayinterp(xarr, dimname, chnk_time):
    """
    Runs xarr interp on an individual chunk, extrapolating to cover boundary case

    Parameters
    ----------
    xarr: xarray DataArray or Dataset, object to be interpolated
    dimname: str, dimension name to interpolate
    chnk_time: xarray DataArray, time to interpolate to

    Returns
    -------
    Interpolated xarr object.

    """
    if dimname == 'time':
        return xarr.interp(time=chnk_time, method='linear', assume_sorted=True,
                           kwargs={'bounds_error': True, 'fill_value': 'extrapolate'})
    else:
        raise NotImplementedError('Only "time" currently supported dim name')


def _interp_across_chunks_construct_times(xarr, new_times, dimname):
    """
    Takes in the existing xarray dataarray/dataset (xarr) and returns chunk indexes and times that allow for
    interpolating to the desired xarray dataarray/dataset (given as new_times).  This allows us to interp across
    the dask array chunks without worrying about boundary cases between worker blocks.

    Parameters
    ----------
    xarr: xarray DataArray or Dataset, object to be interpolated
    new_times: xarray DataArray, times for the array to be interpolated to
    dimname: str, dimension name to interpolate

    Returns
    -------
    chnk_idxs: list of lists, each element is a list containing time indexes for the chunk, ex: [[0,2000], [2000,4000]]
    chnkwise_times: list or DataArray, each element is the section of new_times that applies to that chunk

    """
    chnkwise_times = []
    chnk_idxs = []
    prev_chnk = 0
    try:
        xarr_chunks = xarr.chunks[0]    # works for xarray DataArray
    except KeyError:
        xarr_chunks = xarr.chunks[dimname]      # works for xarray Dataset

    for cnt, chnk in enumerate(xarr_chunks):
        chnk_start = prev_chnk
        chnk_end = prev_chnk + chnk
        # add a buffer onto the end of each chunk to be able to interp across chunk boundary
        min_chnk_time = float(xarr.isel({dimname: slice(chnk_start, chnk_end + 1)}).time.min())
        max_chnk_time = float(xarr.isel({dimname: slice(chnk_start, chnk_end + 1)}).time.max())
        if cnt == 0:
            # for the first chunk, find out if the xarr data even covers the new_times.  If not, extrapolate
            min_xarr_time = float(xarr[dimname].min())
            min_interp_time = float(new_times.min())
            if min_interp_time < min_xarr_time:
                print('Extrapolating first data point back to the min desired interpolation time')
                print('First data point time: {}, First desired interp time: {}'.format(min_xarr_time,
                                                                                        min_interp_time))
                print('Extrapolated Time: {}'.format(min_xarr_time - min_interp_time))
                min_chnk_time = min_interp_time
        if cnt == len(xarr_chunks) - 1 or len(xarr_chunks) == 1:
            # for the last chunk, find out if the xarr data even covers the new_times.  If not, extrapolate
            #  - Check the first chunk too if this is a one chunk array
            max_xarr_time = float(xarr[dimname].max())
            max_interp_time = float(new_times.max())
            if max_xarr_time < max_interp_time:
                print('Extrapolating last data point up to the max desired interpolation time')
                print('Last data point time: {}, Last desired interp time: {}'.format(max_xarr_time,
                                                                                      max_interp_time))
                print('Extrapolated Time: {}'.format(max_interp_time - max_xarr_time))
                max_chnk_time = max_interp_time
        chnk_idxs.append([chnk_start, chnk_end + 1])

        chnkwise_times.append(
            new_times.where(new_times >= min_chnk_time, drop=True).where(new_times <= max_chnk_time, drop=True))
        prev_chnk += chnk

    # only return chunk blocks that have valid times in them
    empty_chunks = [chnkwise_times.index(i) for i in chnkwise_times if i.size == 0]
    chnk_idxs = [i for i in chnk_idxs if chnk_idxs.index(i) not in empty_chunks]
    chnkwise_times = [i for i in chnkwise_times if chnkwise_times.index(i) not in empty_chunks]
    return chnk_idxs, chnkwise_times


def interp_across_chunks(xarr, new_times, dimname='time', daskclient=None):
    """
    Takes in xarr and interpolates to new_times.  Ideally we could use xarray interp_like or interp, but neither
    of these are implemented with support for chunked dask arrays.  Therefore, we have to determine the times of
    each chunk and interpolate individually.  To allow for the case where a value is between chunks or right on
    the boundary, we extend the chunk time to buffer the gap.

    Parameters
    ----------
    xarr: xarray DataArray or Dataset, object to be interpolated
    new_times: xarray DataArray, times for the array to be interpolated to
    dimname: str, dimension name to interpolate
    daskclient: dask.distributed.client or None, if running outside of dask cluster

    Returns
    -------
    newarr: xarray DataArray or Dataset, interpolated xarr
    """
    if type(xarr) not in [xr.DataArray, xr.Dataset]:
        raise NotImplementedError('Only xarray DataArray and Dataset objects allowed.')
    if len(list(xarr.dims)) > 1:
        raise NotImplementedError('Only one dimensional data is currently supported.')

    chnk_idxs, chnkwise_times = _interp_across_chunks_construct_times(xarr, new_times, dimname)
    xarrs_chunked = [xarr.isel({dimname: slice(i, j)}).chunk(j-i,) for i, j in chnk_idxs]
    if daskclient is None:
        interp_arrs = []
        for ct, xar in enumerate(xarrs_chunked):
            interp_arrs.append(_interp_across_chunks_xarrayinterp(xar, dimname, chnkwise_times[ct]))
        newarr = xr.concat(interp_arrs, dimname)
    else:
        interp_futs = daskclient.map(_interp_across_chunks_xarrayinterp, xarrs_chunked, [dimname] * len(chnkwise_times),
                                     chnkwise_times)
        newarr = daskclient.submit(xr.concat, interp_futs, dimname).result()
    return newarr


def clear_data_vars_from_dataset(dataset, datavars):
    """
    Some code to handle dropping data variables from xarray Datasets in different containers.  We use lists of Datasets,
    dicts of Datasets and individual Datasets in different places.  Here we can just pass in whatever, drop the
    variable or list of variables, and get the Dataset back.

    Parameters
    ----------
    dataset: xarray Dataset, list, or dict of xarray Datasets
    datavars: str or list, variables we wish to drop from the xarray Dataset

    Returns
    -------
    dataset: original Dataset with dropped variables

    """
    if type(datavars) == str:
        datavars = [datavars]

    for datavar in datavars:
        if type(dataset) == dict:  # I frequently maintain a dict of datasets for each sector
            for sec_ident in dataset:
                if datavar in dataset[sec_ident].data_vars:
                    dataset[sec_ident] = dataset[sec_ident].drop_vars(datavar)
        elif type(dataset) == list:  # here if you have lists of Datasets
            for cnt, dset in enumerate(dataset):
                if datavar in dset.data_vars:
                    dataset[cnt] = dataset[cnt].drop_vars(datavar)
        elif type(dataset) == xr.Dataset:
            if datavar in dataset.data_vars:
                dataset = dataset.drop_vars(datavar)
    return dataset


def stack_nan_array(dataarray, stack_dims=('time', 'beam')):
    orig_idx = np.where(~np.isnan(dataarray))
    dataarray_stck = dataarray.stack(stck=stack_dims)
    dataarray_stck = dataarray_stck[~np.isnan(dataarray_stck)]
    return orig_idx, dataarray_stck


def reform_nan_array(dataarray_stack, orig_idx, orig_shape, orig_coords, orig_dims):
    """
    To handle NaN values in our input arrays, we flatten and index only the valid values.  Here we rebuild the
    original square shaped arrays we need using one of the original arrays as reference.

    Parameters
    ----------
    arr: xarray DataArray, flattened array that we just interpolated
    ref_xarray: xarray DataArray, array provided just so we can scrape the shape and coordinate
    orig_idx: tuple, 2 elements, one for 1st dimension indexes and one for 2nd dimension indexes, see np.where

    Returns
    -------
    final_arr: xarray DataArray, values of arr, filled to be square with NaN values, coordinates of ref_array

    """
    final_arr = np.empty(orig_shape)
    final_arr[:] = np.nan
    final_arr[orig_idx] = dataarray_stack
    final_arr = xr.DataArray(final_arr, coords=orig_coords, dims=orig_dims)
    return final_arr
