import os
import numpy as np
from dask.distributed import get_client, Client, Lock
from fasteners import InterProcessLock


class DaskProcessSynchronizer:
    """Provides synchronization using file locks via the
    `fasteners <http://fasteners.readthedocs.io/en/latest/api/process_lock.html>`_
    package.
    Parameters
    ----------
    path : string
        Path to a directory on a file system that is shared by all processes.
        N.B., this should be a *different* path to where you store the array.
    """

    def __init__(self, path):
        self.path = path

    def __getitem__(self, item):
        path = os.path.join(self.path, item)
        try:  # this will work if dask.distributed workers exist
            lock = Lock(path)
        except AttributeError:  # otherwise default to the interprocesslock used by zarr
            lock = InterProcessLock(path)
        return lock


def dask_find_or_start_client(address=None, silent=False):
    """
    Either start or return Dask client in local/networked cluster mode

    Parameters
    ----------
    address: str, ip address for existing or desired new dask server instance
    silent: bool, whether or not to print messages

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
            # client = Client(processes=False)
            # client = Client(threads_per_worker=1)
            client = Client()
            if not silent:
                print('Started local cluster client...')
        else:
            client = Client(address=address)
            if not silent:
                print('Started client on address {}...'.format(address))
    return client


def get_max_cluster_allocated_memory(client):
    """
    Retrieve the max memory across all workers in the cluster added together

    Parameters
    ----------
    client: dask.distributed.Client

    Returns
    -------
    float, sum of max memory across all workers

    """
    worker_ids = list(client.scheduler_info()['workers'].keys())
    mem_per_worker = [client.scheduler_info()['workers'][wrk]['memory_limit'] for wrk in worker_ids]
    return np.sum(mem_per_worker) / (1024 ** 3)


def get_number_of_workers(client):
    """
    Retrieve the total number of workers from the dask cluster

    Parameters
    ----------
    client: dask.distributed.Client

    Returns
    -------
    int, total number of workers

    """
    return len(client.scheduler_info()['workers'])


def split_array_by_number_of_workers(client, dataarray, max_len=None):
    """
    In order to operate on an array in a parallelized way, we need to split the array into equal chunks to pass to each
    worker.  Here we do that by just dividing by the number of workers.

    Optional parameter is to restrict the size of the chunks by an int max_len.  This of course only applies if the
    chunks were going to be larger than max_len anyway.

    Drop empty if the length of the array is greater than the number of workers.

    Parameters
    ----------
    client: dask.distributed.Client
    dataarray: xarray Dataarray, one dimensional array
    max_len: int, max number of values per chunk, if None, ignored

    Returns
    -------
    data_out: list of numpy arrays representing chunks of the original array
    data_idx: list of numpy arrays representing indexes of new values from original array

    """
    numworkers = get_number_of_workers(client)
    split = None

    if max_len is not None:
        if len(dataarray) > max_len * numworkers:  # here we apply max_len, but only if necessary based on the length of the array
            max_split_count = np.ceil(len(dataarray) / max_len)  # round up to ensure you are below max_len
            split = np.array_split(dataarray, max_split_count)
    if split is None:
        split = np.array_split(dataarray, numworkers)
    data_out = [s for s in split if s.size != 0]

    # search sorted to build the index gets messy with very long arrays and/or lots of splits. Plus we should just know
    #   the index without having to search for it....
    # data_idx = np.searchsorted(dataarray, data_out)
    data_idx = []
    cnt = 0
    for i in data_out:
        datalen = len(i)
        data_idx.append(np.arange(cnt, datalen + cnt, 1))
        cnt += datalen

    return data_out, data_idx
