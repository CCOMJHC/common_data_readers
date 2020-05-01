import os
from time import perf_counter
import xarray as xr
import pandas as pd
import numpy as np
from dask.distributed import wait
from pyproj import Geod

from xarray_conversion import BatchRead
from xarray_helpers import interp_across_chunks, clear_data_vars_from_dataset, stack_nan_array, reform_nan_array
from dask_helpers import dask_find_or_start_client, split_array_by_number_of_workers
from rotations import build_mounting_angle_mat, build_rot_mat, combine_rotation_matrix
from svcorrect import distributed_run_sv_correct, SoundSpeedProfile, get_sv_files_from_directory, \
    return_supported_casts_from_list


debug = False


class Fqpr:
    """
    Fully qualified ping record: contains all records built from the raw MBES file and supporting data files

    Parameters
    ----------
    source_dat: instance of xarray_conversion BatchRead class
    source: string, drives the read_from_source method to access source_dat
    address: string or None, passed to dask_find_or_start_client to setup dask cluster

    """
    def __init__(self, source_dat=None, source='Kongsberg_all', address=None):
        self.source = source
        self.source_dat = source_dat
        self.fqpr = None
        self.client = None
        self.address = address
        self.cast_files = None
        self.soundspeedprofiles = None

        self.tx_vecs = None
        self.rx_vecs = None
        self.tx_reversed = False
        self.rx_reversed = False
        self.ideal_tx_vec = None
        self.ideal_rx_vec = None

    def read_from_source(self):
        """
        Activate rawdat object's appropriate read class
        """
        if self.source == 'Kongsberg_all' and self.source_dat is not None:
            self.client = self.source_dat.client  # mbes read is first, pull dask distributed client from it
            if self.source_dat.raw_rangeangle is None:
                self.source_dat.read()
        else:
            self.client = dask_find_or_start_client(address=self.address)

    def generate_starter_orientation_vectors(self, txrx, tstmp):
        """
        Take in identifiers to find the correct xyzrph entry, and use the heading value to figure out if the
        transmitter/receiver (tx/rx) is oriented backwards.  Otherwise return ideal vectors for representation of
        the tx/rx.

        Parameters
        ----------
        txrx: list, transmit/receive identifiers for xyzrph dict (['tx', 'rx'])
        tstmp: float, timestamp for the appropriate xyzrph entry

        Returns
        -------
        self.ideal_tx_vec: numpy array, 3d vector orientation of the transmitter pre-rotation
        self.ideal_rx_vec: numpy array, 3d vector orientation of the receiver pre-rotation

        """
        # start with ideal vectors, flip sign if tx/rx is installed backwards
        # ideal tx vector is aligned with the x axis (forward)
        tx_heading = abs(float(self.source_dat.xyzrph[txrx[0] + '_h'][tstmp]))
        if (tx_heading > 90) and (tx_heading < 270):
            self.ideal_tx_vec = np.array([-1, 0, 0])
            self.tx_reversed = True
        else:
            self.ideal_tx_vec = np.array([1, 0, 0])

        # ideal rx vector is aligned with the y axis (stbd)
        rx_heading = abs(float(self.source_dat.xyzrph[txrx[1] + '_h'][tstmp]))
        if (rx_heading > 90) and (rx_heading < 270):
            self.ideal_rx_vec = np.array([0, -1, 0])
            self.rx_reversed = True
        else:
            self.ideal_rx_vec = np.array([0, 1, 0])

    def return_attitude_rotation_matrix(self, at_time, time_index=None):
        """
        We start of doing calculations in array relative reference frame.  To get to a geographic reference frame, we
        need rotation matrices for attitude and mounting angles.  Here we construct the attitude rotation matrix, at
        specific times (time of ping, time of receive, etc.).  We also allow for additional selection to accommodate
        for when we get duplicate times due to pingtime + twtt sometimes getting the same time for some beams.

        Parameters
        ----------
        at_time: xarray Dataarray, 1 dimensional array representing times we want to draw attitude from, must have time
                 coordinate
        time_index: numpy array, optional, if provided, is a 1 dimensional array of integers representing the index of
                    values you want to return from the attitude Dataset

        Returns
        -------
        att_times: xarray Dataarray, 1 dimensional array representing attitude times
        attitude_rotation: xarray DataArray, 3 dims (time, x, y) containing rot matrix at each time

        """
        # generate rotation matrices for the transmit array at time of ping
        att = interp_across_chunks(self.source_dat.raw_att, at_time, daskclient=self.client)
        if time_index is not None:
            att = att.isel(time=time_index)
        att_times = att.time
        attitude_rotation = build_rot_mat(att['roll'], att['pitch'], att['heading'], order='rpy', degrees=True)
        return att_times, attitude_rotation

    def return_mounting_rotation_matrix(self, xyzrph_record, prefix, tstmp):
        """
        Using the xyzrph record from xarray_conversion generated rangeangle DataSet, build the static mounting angle
        rotation matrix, pulling from the appropriate timestamp

        Parameters
        ----------
        xyzrph_record: dict, contains the user entered roll/pitch/yaw mounting angles
        prefix: string, the prefix identifier for the xyzrph dict, lets you specify which rpy you want
        tstmp: string, key for the dict you get from xyzrph, which has keys equal to the timestamps of each entry
               in the xyzrph dict

        Returns
        -------
        mount_rotation: xarray DataArray, 3dim (time, x, y) rotation matrix composed of rpy rotations at time of xyzrph
                                         record (length of time dim is always 1)

        """
        mount_rotation = build_mounting_angle_mat(float(xyzrph_record[prefix + '_r'][tstmp]),
                                                  float(xyzrph_record[prefix + '_p'][tstmp]),
                                                  float(xyzrph_record[prefix + '_h'][tstmp]), tstmp)
        return mount_rotation

    def get_ping_times(self, rangeangle, idx):
        """
        Given a rangeangle Dataset and an index of values that we are interested in from the Dataset, return the ping
        time, which is just the time coordinate of the rangeangle Dataset

        Parameters
        ----------
        rangeangle: xarray DataSet, ndim DataSet containing all relevant records from ping
        idx: xarray Dataarray, times of interest from the DataSet

        Returns
        -------
        tx_tstmp_idx: xarray Dataarray, 1 dim ping times from the DataSet

        """
        tx_tstmp_idx = rangeangle.time.where(idx, drop=True)
        return tx_tstmp_idx

    def get_receive_times(self, pingtime, twtt):
        """
        Given ping time and two way travel time, return the time of receive for each beam.  Provides unique times, as
        by just adding ping time and twtt, you might end up with duplicate times.  To get back to beamwise times, an
        index is provided that you can select by.

        Parameters
        ----------
        pingtime: xarray Dataarray, 1dim array of timestamps representing time of ping
        twtt: xarray Dataarray, 2dim (time/beam) array of timestamps representing time traveling through water for each
              beam

        Returns
        -------
        rx_tstmp_idx: tuple of numpy arrays, 2d indices of original rx_tstmp Dataarray, used to reconstruct array
        inv_idx: numpy array, 1d indices of time of receive that can be used to reconstruct non-unique timestamps
        rx_interptimes: xarray Dataarray, 1 dim array of timestamps for unique times of receive

        """
        rx_tstmp = pingtime + twtt
        rx_tstmp_idx, rx_tstmp_stck = stack_nan_array(rx_tstmp, stack_dims=('time', 'beam'))
        unique_rx_times, inv_idx = np.unique(rx_tstmp_stck.values, return_inverse=True)
        rx_interptimes = xr.DataArray(unique_rx_times, coords=[unique_rx_times], dims=['time']).chunk()
        return rx_tstmp_idx, inv_idx, rx_interptimes

    def handle_reversed_arrays(self, tx_angle, rx_angle):
        """
        from notes in Glen's par module:

        TX is reverse mounted: subtract 180 from heading installation angle and flip sign of the pitch offset.
        RX is reverse mounted: subtract 180 from heading installation angle and flip the sign of the beam
            steering angles and the sign of the receiver roll offset.  ~ per Jonny B 20120928

        Parameters
        ----------
        tx_angle: xarray Dataarray, transmitter tiltangle on ping, 1 dim with coordinates=time
        rx_angle: xarray Dataarray, receiver beam pointing angle, 1 dim with coordinates=time

        Returns
        -------
        tx_angle: xarray Dataarray, transmitter tiltangle on ping (sign corrected)
        rx_angle: xarray Dataarray, receiver beam pointing angle (sign corrected)
        """

        if self.rx_reversed:
            rx_angle = -rx_angle
        if self.tx_reversed:
            tx_angle = -tx_angle
        return tx_angle, rx_angle

    def construct_array_relative_beamvector(self, maintx, mainrx, tx_angle, rx_angle):
        """
        Given the orientation vectors representing the transmitter/receiver at time of ping/receive (maintx, mainrx) and
        the TX/RX steering angles (tx_angle, rx_angle), determine new 3d beam vector components at the midpoint between
        the TX and RX.  This would be the 'actual' array relative beam vector.

        This is a simplification of the actual scenario, adding error in the xyz due to the difference in path length/
        direction of the actual ray from tx-seafloor and seafloor-rx and this co-located assumption (tx-seafloor and
        rx-seafloor are the same is the assumption)

        x = +FORWARD, y=+STARBOARD, z=+DOWN

        Parameters
        ----------
        maintx = orientation vector for transmitter at time of transmit
        mainrx = orientation vector for receiver at time of receive
        tx_angle: xarray Dataarray, transmitter tiltangle on ping
        rx_angle: xarray Dataarray, receiver beam pointing angle

        Returns
        -------
        beamvecs: xarray Dataarray, 3d beam vector in co-located array ref frame.  Of shape (xyz, time, beam), with 10
                  times and 200 beams, beamvecs shape would be (3, 10, 200)

            <xarray.DataArray 'tiltangle' (xyz: 3, time: 10, beam: 200)>
            dask.array<concatenate, shape=(3, 10, 200), dtype=float64, chunksize=(1, 10, 200), chunktype=numpy.ndarray>
            Coordinates:
              * time     (time) float64 1.496e+09 1.496e+09 ...
              * beam     (beam) int32 0 1 2 3 4 5 6 7 8 ... 194 195 196 197 198 199 200
              * xyz      (xyz) object 'x' 'y' 'z'

        """
        print('Generating alignment angles between vectors...')
        # delta - alignment angle between tx/rx vecs
        delt = np.arccos(xr.dot(maintx, mainrx, dims=['xyz'])) - np.pi / 2
        ysub1 = -np.sin(rx_angle)

        print('Building starter 3d beam vector...')
        # solve for components of 3d beam vector
        ysub1 = ysub1 / np.cos(delt)
        ysub2 = np.sin(tx_angle) * np.tan(delt)
        radial = np.sqrt((ysub1 + ysub2) ** 2 + np.sin(tx_angle) ** 2)
        x = np.sin(tx_angle)
        y = ysub1 + ysub2
        z = np.sqrt(1 - radial ** 2)

        # generate new dataarray object for beam vectors
        newx, _ = xr.broadcast(x, y)  # broadcast to duplicate x along beam dimension
        beamvecs = xr.concat([newx, y, z], pd.Index(list('xyz'), name='xyz'))
        return beamvecs

    def return_array_geographic_rotation(self, maintx, mainrx):
        """
        Use the transmitter/receiver array orientations to build a rotation matrix between the geographic/array rel
        reference frame.

        Parameters
        ----------
        maintx = orientation vector for transmitter at time of transmit, 2dim of shape (time, xyz)
        mainrx = orientation vector for receiver at time of receive, 2dim of shape (time, xyz)

        Returns
        -------
        rotgeo: xarray Dataarray, rotation matrices at each time/beam, of shape (beam, rot_i, time, xyz)

        <xarray.DataArray 'getitem-82dd48467b1f4e8b4f56bbe5e841cc9f' (beam: 182, rot_i: 3, time: 2, xyz: 3)>
        dask.array<transpose, shape=(182, 3, 2, 3), dtype=float64, chunksize=(182, 3, 2, 1), chunktype=numpy.ndarray>
        Coordinates:
          * rot_i    (rot_i) int32 0 1 2
          * time     (time) float64 1.496e+09 1.496e+09
          * beam     (beam) int32 0 1 2 3 4 5 6 7 8 ... 174 175 176 177 178 179 180 181
          * xyz      (xyz) <U1 'x' 'y' 'z'

        """
        print('Construct rotation matrix between locally level/geographic coord system...')
        # build rotation matrix for going from locally level to geographic coord sys
        x_prime = maintx
        z_prime = cross(x_prime, mainrx, 'xyz')
        y_prime = cross(z_prime, x_prime, 'xyz')
        rotgeo = xr.concat([x_prime, y_prime, z_prime], pd.Index([0, 1, 2], name='rot_j')).T
        # to do the dot product correctly, you need to align the right dimension in both matrices by giving
        # them the same name (xyz for rotgeo and bv_geo in this case)
        rotgeo = rotgeo.rename({'xyz': 'rot_i'})
        rotgeo.coords['rot_i'] = [0, 1, 2]
        rotgeo = rotgeo.rename({'rot_j': 'xyz'})
        rotgeo.coords['xyz'] = ['x', 'y', 'z']
        return rotgeo

    def build_geographic_beam_vectors(self, rotgeo, beamvecs):
        """
        Apply rotation matrix to bring transducer rel. beam vectors to geographic ref frame

        Parameters
        ----------
        rotgeo: xarray Dataarray, see return_array_geographic_rotation
        beamvecs: xarray Dataarray, see construct_array_relative_beamvector

        Returns
        -------
        bv_geo: xarray Dataarray, beam vectors in geographic ref frame, of shape (time, beam, bv_xyz)

        """
        bv_geo = xr.dot(rotgeo, beamvecs, dims='xyz')
        bv_geo = bv_geo.rename({'rot_i': 'bv_xyz'})
        bv_geo.coords['bv_xyz'] = ['x', 'y', 'z']
        bv_geo = bv_geo.transpose('time', 'beam', 'bv_xyz')
        return bv_geo

    def compute_relative_azimuth(self, bv_geo, heading):
        """
        Compute the relative azimuth from array to end of beam vector in geographic ref frame

        Parameters
        ----------
        bv_geo: xarray Dataarray, see build_geographic_beam_vectors
        heading: xarray Dataarray, 1 dim array of heading values, coords=time

        Returns
        -------
        rel_azimuth: xarray Datarray, 2dim (time, beam), beam-wise beam azimuth values relative to vessel heading at
                     time of ping

        """
        print('Computing beam azimuth/angle....')
        # derive azimuth/angle from the newly created geographic beam vectors
        bv_azimuth = np.rad2deg(np.arctan2(bv_geo.sel(bv_xyz='y'), bv_geo.sel(bv_xyz='x')))
        rel_azimuth = np.deg2rad((bv_azimuth - heading + 360) % 360)
        return rel_azimuth

    def compute_geo_beam_pointing_angle(self, bv_geo, rx_angle):
        """
        Build new beam pointing angle (rel to the vertical) and with the correct sign (+ to starboard) in the geographic
        ref frame.

        Parameters
        ----------
        bv_geo: xarray Dataarray, see build_geographic_beam_vectors
        rx_angle: xarray Dataarray, receiver beam pointing angle (sign corrected)

        Returns
        -------
        new_pointing_angle: xarray Dataarray, 2 dim (time, beam) values for beampointingangle at each beam

        """
        bvangle_divisor = np.sqrt(np.square(bv_geo.sel(bv_xyz='x')) + np.square(bv_geo.sel(bv_xyz='y')))
        # new pointing angle is equal to pi/2 - depression angle (depression angle relative to horiz, pointing
        #    angle is the incidence angle relative to vertical)
        new_pointing_angle = (np.pi / 2) - np.arctan(bv_geo.sel(bv_xyz='z') / bvangle_divisor)
        # flip the sign where the azimuth is pointing to port, allows us to maintain which side the angle is on
        newindx = np.ones_like(new_pointing_angle)
        newindx = np.negative(newindx, out=newindx, where=rx_angle < 0)
        new_pointing_angle = new_pointing_angle * newindx
        return new_pointing_angle

    def get_cast_files(self, src):
        """
        Load to self.cast_files the file paths to the sv casts of interest.

        Parameters
        ----------
        src: list/string, either a list of files to include or the path to a directory containing files

        Returns
        -------
        self.cast_files: list, list of paths to cast files with only unique entries

        """
        if type(src) is str:
            svfils = get_sv_files_from_directory(src)
        elif type(src) is list:
            svfils = return_supported_casts_from_list(src)
        else:
            raise TypeError('Provided source is neither a path or a list of files.  Please provide one of those.')

        if self.cast_files is None:
            self.cast_files = svfils
        else:
            self.cast_files.extend(sv for sv in svfils if sv not in self.cast_files)

    def setup_casts(self, surf_sound_speed, z_pos, add_cast_files=None):
        """
        Using all the profiles in the rangeangle dataset as well as externally provided casts as files, generate
        SoundSpeedProfile objects and build the lookup tables.

        Parameters
        ----------
        surf_sound_speed: xarray DataArray, 1dim array of surface sound speed values, coords = timestamp
        z_pos: float, z value of the transducer position in the watercolumn from the waterline
        add_cast_files: list/str, optional - either a list of sv files or the path to a directory of files

        Returns
        -------
        list, a list of SoundSpeedProfile objects with constructed lookup tables

        """
        # get the svp files and the casts in the mbes converted data
        if add_cast_files is not None:
            self.get_cast_files(add_cast_files)
        mbes_profs = self.source_dat.return_all_profiles()

        # convert to SoundSpeedProfile objects
        rangeangle_casts = []
        if mbes_profs:
            for castname, data in mbes_profs.items():
                try:
                    casttime = float(castname.split('_')[1])
                    cst_object = SoundSpeedProfile(data, z_pos, surf_sound_speed, prof_time=casttime,
                                                   prof_type='raw_rangeangle')
                    cst_object.generate_lookup_table()
                    rangeangle_casts.append(cst_object)
                except ValueError:
                    raise ValueError('Profile attribute name in range_angle DataSet must include timestamp, ex: '
                                     '"profile_1495599960")')

        # include additional casts from sv files as SoundSpeedProfile objects
        additional_casts = []
        if self.cast_files is not None:
            for f in self.cast_files:
                cst_object = SoundSpeedProfile(f, z_pos, surf_sound_speed, prof_type='caris_svp')
                cst_object.generate_lookup_table()
                additional_casts.append(cst_object)

        # retain all the casts that are unique in time, preferring the svp file ones (they have location and stuff)
        final_casts = additional_casts
        cast_tstmps = [cst.prof_time for cst in final_casts]
        new_casts = [cst for cst in rangeangle_casts if cst.prof_time not in cast_tstmps]
        return final_casts + new_casts

    def setup_casts_for_sec_by_index(self, sector_index, add_cast_files=None):
        """
        Generate cast objects for the given sector across all values given for waterline

        Originally started with building all casts for the first sector and using those cast objects across all other
        sectors (as the z pos is basically the same) but ran into the issue where different sectors would start at
        different times in the file.

        Parameters
        ----------
        sector_index: int, index of sector
        add_cast_files: list/str, optional - either a list of sv files or the path to a directory of files

        Returns
        -------
        list, a list of lists of SoundSpeedProfile objects with constructed lookup tables for each waterline value in
              installation parameters

        """
        # generate cast objects for the first sector across all values given for waterline, assume z pos across sectors
        #    is the same
        casts = []
        sectors = self.source_dat.return_sector_time_indexed_array()
        for applicable_index, timestmp, prefixes in sectors[sector_index]:
            ss_by_sec = self.source_dat.raw_rangeangle[sector_index].soundspeed.where(applicable_index, drop=True)
            z_pos = float(self.source_dat.xyzrph['waterline'][timestmp]) + \
                    float(self.source_dat.xyzrph[prefixes[0] + '_z'][timestmp])
            casts.extend(self.setup_casts(self.source_dat.raw_rangeangle[sector_index].soundspeed, z_pos, add_cast_files))
        return casts

    def return_cast_idx_nearestintime(self, casts, idx, pings_per_chunk):
        """
        Need to find the cast associated with each chunk of data.  Currently we just take the average chunk time and
        find the closest cast time, and assign that cast.  We also need the index of the chunk in the original size
        dataset, as we built the casts based on the original size soundvelocity dataarray.

        Parameters
        ----------
        casts: list of SoundSpeedProfile objects
        idx: xarray Dataarray, the applicable_index generated from return_sector_time_indexed_array
        pings_per_chunk: int, number of pings in each worker chunk

        Returns
        -------
        data: list of lists, each sub-list is [timestamps for the chunk, index of the chunk in the original array, cast
               that applies to that chunk]

        """
        timevals = idx.time[idx]
        index_timevals = np.arange(len(idx))[idx]
        if len(timevals) < pings_per_chunk:
            times_by_chunk = [timevals]
            idx_by_chunk = [index_timevals]
        else:
            max_split_count = np.ceil(len(timevals) / pings_per_chunk)  # round up to ensure you are below max_len
            times_by_chunk = np.array_split(timevals, max_split_count)
            idx_by_chunk = np.array_split(index_timevals, max_split_count)

        data = []
        for cnt, tme in enumerate(times_by_chunk):
            avgtme = float(timevals.mean())
            cst = casts[np.argmin([np.abs(c.prof_time - avgtme) for c in casts])]
            data.append([tme, idx_by_chunk[cnt], cst])
        return data

    def return_additional_xyz_offsets(self, prefixes, sec_info, timestmp):
        """
        All the kongsberg sonars have additional offsets in the installation parameters document listed as the difference
        between the measured center of the transducer and the phase center of the transducer.  Here we get those values
        for the provided system (we've previously stored them in the xyzrph data)

        Parameters
        ----------
        prefixes: str, prefix identifier for the tx/rx, will vary for dual head systems
        sec_info: str, sector identifier, see parse_sect_info_from_identifier
        timestmp: str, timestamp for the appropriate xyzrph record

        Returns
        -------
        addtl_x: float, additional x offset
        addtl_y: float, additional y offset
        addtl_z: float, additional z offset

        """
        x_off_ky = prefixes[0] + '_x_' + sec_info['sector']
        if x_off_ky in self.source_dat.xyzrph:
            addtl_x = float(self.source_dat.xyzrph[x_off_ky][timestmp])
        else:
            addtl_x = 0

        y_off_ky = prefixes[0] + '_y_' + sec_info['sector']
        if y_off_ky in self.source_dat.xyzrph:
            addtl_y = float(self.source_dat.xyzrph[y_off_ky][timestmp])
        else:
            addtl_y = 0

        z_off_ky = prefixes[0] + '_z_' + sec_info['sector']
        if z_off_ky in self.source_dat.xyzrph:
            addtl_z = float(self.source_dat.xyzrph[z_off_ky][timestmp])
        else:
            addtl_z = 0
        return [addtl_x, addtl_y, addtl_z]

    def get_orientation_vectors(self):
        """
        Using attitude angles, mounting angles, build the tx/rx vectors that
        represent the orientation of the tx/rx at time of transmit/receive

        Returns
        -------
        self.main_actual_tx_vec = orientation vector for transmitter at time of transmit
        self.main_actual_rx_vec = orientation vector for receiver at time of receive
        self.sec_actual_tx_vec/self.sec_actual_rx_vec are populated for dual head systems
        """

        print('\n****Building tx/rx vectors at time of transmit/receive****')
        starttime = perf_counter()
        self.tx_vecs = {}
        self.rx_vecs = {}

        sectors = self.source_dat.return_sector_time_indexed_array()
        for s_cnt, sector in enumerate(sectors):
            for applicable_index, timestmp, prefixes in sector:
                self.generate_starter_orientation_vectors(prefixes, timestmp)
                ra = self.source_dat.raw_rangeangle[s_cnt]
                twtt = self.source_dat.select_array_from_rangeangle('traveltime', ra.sector_identifier)
                twtt = twtt.where(applicable_index, drop=True)

                # generate rotation matrices for the transmit array at time of ping
                tx_tstmp_idx = self.get_ping_times(ra, applicable_index)
                tx_att_times, tx_attitude_rotation = self.return_attitude_rotation_matrix(tx_tstmp_idx.compute())

                # generate rotation matrices for receive array at time of receive, ping + twtt
                rx_tstmp_idx, inv_idx, rx_interptimes = self.get_receive_times(tx_tstmp_idx, twtt)
                rx_att_times, rx_attitude_rotation = self.return_attitude_rotation_matrix(rx_interptimes,
                                                                                          time_index=inv_idx)

                print('Build orientation matrices for tx/rx mounting angles...')
                # Build orientation matrices for mounting angles
                tx_mount_rotation = self.return_mounting_rotation_matrix(self.source_dat.xyzrph, prefixes[0], timestmp)
                rx_mount_rotation = self.return_mounting_rotation_matrix(self.source_dat.xyzrph, prefixes[1], timestmp)

                final_tx_rot = combine_rotation_matrix(tx_mount_rotation, tx_attitude_rotation)
                final_rx_rot = combine_rotation_matrix(rx_mount_rotation, rx_attitude_rotation)

                final_tx_vec = xr.DataArray(final_tx_rot.data @ self.ideal_tx_vec,
                                            coords={'time': tx_att_times, 'xyz': ['x', 'y', 'z']},
                                            dims=['time', 'xyz'])
                final_rx_vec = xr.DataArray(final_rx_rot.data @ self.ideal_rx_vec,
                                            coords={'time': rx_att_times, 'xyz': ['x', 'y', 'z']},
                                            dims=['time', 'xyz'])
                final_rx_vec = reform_nan_array(final_rx_vec, rx_tstmp_idx, twtt.shape + (3,),
                                                [twtt.coords['time'], twtt.coords['beam'], final_rx_vec['xyz']],
                                                twtt.dims + ('xyz',))

                print('Generate rotated vectors in local coord system using orientation/attitude...')
                # generate tx/rx orientation vectors at time of transmit/receive in local coord system
                # ensure chunks include the whole xyz vector, to avoid operations 'across core dimension', i.e. with
                #    workers having different parts of the same vector
                if ra.sector_identifier not in self.tx_vecs and ra.sector_identifier not in self.rx_vecs:
                    self.tx_vecs[ra.sector_identifier] = final_tx_vec.chunk({'xyz': 3})
                    self.rx_vecs[ra.sector_identifier] = final_rx_vec.chunk({'xyz': 3})
                else:
                    self.tx_vecs[ra.sector_identifier] = xr.concat([self.tx_vecs[ra.sector_identifier],
                                                                    final_tx_vec.chunk({'xyz': 3})], dim='time')
                    self.rx_vecs[ra.sector_identifier] = xr.concat([self.rx_vecs[ra.sector_identifier],
                                                                    final_rx_vec.chunk({'xyz': 3})], dim='time')

        endtime = perf_counter()
        print('****Get Orientation Vectors complete: {}s****\n'.format(round(endtime - starttime), 4))

    def build_beam_pointing_vector(self):
        """
        Beam pointing vector is the beam specific vector that arises from the intersection of the tx ping and rx cone
        of sensitivity.  Points at that area.  Is in the geographic coordinate system, built using the tx/rx at time of
        ping/receive.

        Parameters
        ----------
        tx/rx vectors as built in the get_orientation_vectors method
        rawdat heading/tiltangle = ship heading/tx tilt at time of transmit
        rawdat traveltime, beampointingangle = beam two-way-travel-time and pointing angle

        Returns
        -------
        new fqpr Dataset containing
        - beam_vec = beam vectors in geographic coordinate system
        - beam_azimuth = azimuth rel ships heading
        - beam_depression_angle = incident beam angle from vertical in geographic coordinate system
        """
        print('\n****Building beam specific pointing vectors****')
        starttime = perf_counter()
        self.fqpr = {}

        for sec in self.tx_vecs:
            maintx = self.tx_vecs[sec]
            mainrx = self.rx_vecs[sec]
            relevant_ra = self.source_dat.return_range_angle_by_sector(sec)

            hdng = interp_across_chunks(self.source_dat.raw_att['heading'], relevant_ra.time, daskclient=self.client)

            # main vec (primary head) is accessed using the primary system selection
            print('Loading data for sonar system...')
            twtt = self.source_dat.select_array_from_rangeangle('traveltime', sec)
            rx_angle = np.deg2rad(self.source_dat.select_array_from_rangeangle('beampointingangle', sec))
            tx_angle = np.deg2rad(self.source_dat.select_array_from_rangeangle('tiltangle', sec))
            tx_angle, rx_angle = self.handle_reversed_arrays(tx_angle, rx_angle)

            beamvecs = self.construct_array_relative_beamvector(maintx, mainrx, tx_angle, rx_angle)
            rotgeo = self.return_array_geographic_rotation(maintx, mainrx)
            bv_geo = self.build_geographic_beam_vectors(rotgeo, beamvecs)

            rel_azimuth = self.compute_relative_azimuth(bv_geo, hdng)
            new_pointing_angle = self.compute_geo_beam_pointing_angle(bv_geo, rx_angle)

            newfqpr = xr.Dataset({'beam_azimuth': (['time', 'beam'], rel_azimuth.chunk()),
                                  'beam_pointing_angle': (['time', 'beam'], new_pointing_angle.chunk()),
                                  'two_way_travel_time': (['time', 'beam'], twtt.chunk())},
                                 coords={'time': bv_geo.coords['time'], 'beam': bv_geo.coords['beam']})
            self.fqpr[sec] = newfqpr

        endtime = perf_counter()
        print('****Beam Pointing Vector generation complete: {}s****\n'.format(endtime - starttime))

    def sv_correct(self, pings_per_chunk=300, add_cast_files=None):
        """
        Apply sv cast/surface sound speed to raytrace.  Generates xyz for each beam.
        Currently only supports nearest-in-time for selecting the cast for each chunk.  Each chunk is composed of
           pings_per_chunk size chunks, each chunk is sent to a worker with the appropriate cast for processing
        Returns xyz from waterline

        Parameters
        ----------
        pings_per_chunk: int, number of pings in each worker chunk
        add_cast_files: list/string, either a list of files to include or the path to a directory containing files.
                        These are in addition to the casts in the range_angle dataset.

        Returns
        -------
        alongtrack_offset, acrosstrack_offset, depth_offset variables for each sector

        """
        print('\n****Correcting for sound velocity****')
        starttime = perf_counter()
        self.fqpr = clear_data_vars_from_dataset(self.fqpr, ['alongtrack_offset', 'acrosstrack_offset', 'depth_offset'])

        sectors = self.source_dat.return_sector_time_indexed_array()
        for s_cnt, sector in enumerate(sectors):
            cast_objects = self.setup_casts_for_sec_by_index(s_cnt, add_cast_files)
            sec_ident = self.source_dat.raw_rangeangle[s_cnt].sector_identifier
            sec_info = self.parse_sect_info_from_identifier(sec_ident)
            print('Operating on sector {}'.format(sec_ident))
            for applicable_index, timestmp, prefixes in sector:
                print('Sector {}, installation params {}'.format(sec_ident, timestmp))
                rec = self.fqpr[sec_ident]
                cast_chunks = self.return_cast_idx_nearestintime(cast_objects, applicable_index, pings_per_chunk)
                print('divided into {} data chunks for workers...'.format(len(cast_chunks)))

                data_for_workers = []
                for dat in cast_chunks:
                    data_for_workers.append([dat[2], dat[1],
                                            rec.beam_pointing_angle.where(rec.time == dat[0], drop=True),
                                            rec.two_way_travel_time.where(rec.time == dat[0], drop=True),
                                            rec.beam_azimuth.where(rec.time == dat[0], drop=True)])
                rslt = self.client.map(distributed_run_sv_correct, data_for_workers)
                wait(rslt)
                dat = self.client.gather(rslt)
                del data_for_workers, rslt

                addtl_x, addtl_y, addtl_z = self.return_additional_xyz_offsets(prefixes, sec_info, timestmp)
                new_x = xr.concat([d[0] for d in dat], dim='time') + addtl_x
                new_y = xr.concat([d[1] for d in dat], dim='time') + addtl_y
                new_z = xr.concat([d[2] for d in dat], dim='time') + addtl_z

                if 'depth_offset' not in self.fqpr[sec_ident].data_vars:
                    self.fqpr[sec_ident]['alongtrack_offset'] = new_x
                    self.fqpr[sec_ident]['acrosstrack_offset'] = new_y
                    self.fqpr[sec_ident]['depth_offset'] = new_z
                else:
                    self.fqpr[sec_ident]['alongtrack_offset'] = xr.concat(
                        [self.fqpr[sec_ident]['alongtrack_offset'], new_x], dim='time')
                    self.fqpr[sec_ident]['acrosstrack_offset'] = xr.concat(
                        [self.fqpr[sec_ident]['acrosstrack_offset'], new_y], dim='time')
                    self.fqpr[sec_ident]['depth_offset'] = xr.concat([self.fqpr[sec_ident]['depth_offset'], new_z],
                                                                     dim='time')
                del new_x, new_y, new_z
        endtime = perf_counter()
        print('****Sound Velocity complete: {}s****\n'.format(round(endtime - starttime), 4))

    def georef_across_along_depth(self, ellip='WGS84', vert_ref='waterline',
                                  outfold=r'C:\collab\dasktest\data_dir\EM2040_smallfil\outfolder'):
        """
        In progress

        Returns
        -------

        """
        print('\n****Georeferencing sound velocity corrected beam offsets****')
        starttime = perf_counter()

        g = Geod(ellps=ellip)
        sectors = self.source_dat.return_sector_time_indexed_array()

        for s_cnt, sector in enumerate(sectors):
            ra = self.source_dat.raw_rangeangle[s_cnt]
            sec_ident = ra.sector_identifier
            relevant_fqpr = self.fqpr[sec_ident]
            for applicable_index, timestmp, prefixes in sector:
                tstmps = relevant_fqpr.time.where(applicable_index, drop=True)

                latitude = interp_across_chunks(self.source_dat.raw_nav['latitude'], tstmps, daskclient=self.client)
                longitude = interp_across_chunks(self.source_dat.raw_nav['longitude'], tstmps, daskclient=self.client)
                heading = interp_across_chunks(self.source_dat.raw_att['heading'], tstmps, daskclient=self.client)

                at_idx, alongtrack_stck = stack_nan_array(relevant_fqpr.alongtrack_offset.where(applicable_index, drop=True),
                               stack_dims=('time', 'beam'))
                ac_idx, acrosstrack_stck = stack_nan_array(relevant_fqpr.acrosstrack_offset.where(applicable_index, drop=True),
                                                           stack_dims=('time', 'beam'))
                d_idx, depth_stck = stack_nan_array(relevant_fqpr.depth_offset.where(applicable_index, drop=True),
                                                    stack_dims=('time', 'beam'))

                if vert_ref == 'ellipse':
                    altitude = interp_across_chunks(self.source_dat.raw_nav['altitude'], tstmps, daskclient=self.client)
                    corr_dpth = depth_stck - altitude[d_idx[0]].values
                elif vert_ref == 'vessel':
                    corr_dpth = depth_stck
                elif vert_ref == 'waterline':
                    wline = float(self.source_dat.xyzrph['waterline'][str(timestmp)])
                    corr_dpth = depth_stck - wline

                bm_azimuth = np.rad2deg(np.arctan2(acrosstrack_stck, alongtrack_stck)) + heading[at_idx[0]].values
                bm_radius = np.sqrt(acrosstrack_stck ** 2 + alongtrack_stck ** 2)
                pos = g.fwd(longitude[at_idx[0]].values, latitude[at_idx[0]].values, bm_azimuth.values,
                            bm_radius.values)

                # filter with kongsberg detection info
                if 'detectioninfo' in ra:
                    dinfo = self.source_dat.select_array_from_rangeangle('detectioninfo', ra.sector_identifier)
                    filter_idx, filter_stck = stack_nan_array(dinfo.where(applicable_index, drop=True),
                                                              stack_dims=('time', 'beam'))
                    valid_detections = filter_stck != 2
                    tot = len(filter_stck)
                    tot_valid = np.count_nonzero(valid_detections)
                    tot_invalid = tot - tot_valid
                    print('{}: {} total soundings, {} retained, {} filtered'.format(sec_ident, tot, tot_valid, tot_invalid))
                    x = pos[0][valid_detections]
                    y = pos[1][valid_detections]
                    z = corr_dpth.values[valid_detections]
                else:
                    x = pos[0]
                    y = pos[1]
                    z = corr_dpth.values

                np.savetxt(os.path.join(outfold, sec_ident + '_' + timestmp + '.xyz'),
                           np.c_[x, y, z], ['%3.8f', '%2.8f', '%4.3f'])
        endtime = perf_counter()
        print('****Georeferencing sound velocity corrected beam offsets complete: {}s****\n'.format(round(endtime - starttime), 4))

    def return_sector_ids(self):
        """
        Return a list containing the sector ids across all sectors in the fqpr

        Returns
        -------
        list, sector ids as strings

        """
        return list(self.fqpr.keys())

    def parse_sect_info_from_identifier(self, sector_identifier):
        """
        Take the format used for sector ids and return the encoded info.

        EX: '40111_1_290000' => serial number = 40111, sector index = 1, frequency = 290000 hz

        Parameters
        ----------
        sector_identifier: string, sector ident as string

        Returns
        -------
        dict, dict containing serial number, sector and frequency, all as string

        """
        sec_info = sector_identifier.split('_')
        return {'serial_number': sec_info[0], 'sector': sec_info[1], 'frequency': sec_info[2]}

    def reform_vars_across_sectors_at_time(self, variable_selection, tme, maxbeamnumber=400):
        """
        Take specific variable names and a time, return the array across all sectors merged into one block.

        Do this by finding the ping counter number(s) at that time, and return the full ping(s) associated with that
        ping counter number.

        Interesting Notes:
        - EM2040c will have multiple pings at the same time with different ping counters.  Sometimes there is a slight
            time delay.  Most of the time not.  Tracking ping counter solves this
        - DualHead sonar will have multiple pings at the same time with the same ping counter.  Ugh.  Only way of
            differentiating is through the serial number associated with that sector.


        Parameters
        ----------
        variable_selection: list, variable names you want from the fqpr sectors
        tme: numpy array or float, time to select the dataset by
        maxbeamnumber: int, maximum number of beams for the system

        Returns
        -------
        out: numpy array, data for given variable names at that time for all sectors
        out_sec: numpy array, 1d array containing string values indicating the sector each beam value comes from

        """
        ping_counters = self.source_dat.return_ping_counters_at_time(tme)
        arrlen = len(ping_counters)
        secs = list(self.fqpr.keys())
        if self.source_dat.is_dual_head():
            # dual head sonars will have pings that are the same in time and ping counter.  Only serial number can be
            #    used to distinguish.  All we need to do is ensure primary comes first in the loop.
            primary_sn = str(self.source_dat.raw_rangeangle[0].system_serial_number[0])
            secs.sort(key=lambda x: x.split('_')[0] == primary_sn, reverse=True)
            arrlen = arrlen * 2

        out = np.full((len(variable_selection), arrlen, 9999), np.nan)
        out_sec = np.empty((1, arrlen, 9999), dtype=np.object)

        expected_shape = (len(variable_selection), arrlen, maxbeamnumber)
        expected_sec_shape = (1, arrlen, maxbeamnumber)

        data_strt = np.zeros((len(variable_selection), len(ping_counters) * 2), dtype=int)
        data_end = np.zeros((len(variable_selection), len(ping_counters) * 2), dtype=int)

        for s_cnt, sec in enumerate(secs):
            counter_times = self.source_dat.return_active_sectors_for_ping_counter(s_cnt, ping_counters)
            counter_idxs = np.array(np.where(counter_times != 0)).ravel()

            if np.any(counter_times):
                relevant_fqpr = self.fqpr[sec].sel(time=counter_times[counter_times != 0])
                for v_cnt, dattype in enumerate(variable_selection):
                    dat = relevant_fqpr[dattype]
                    data_end[v_cnt, counter_idxs] += dat.shape[1]
                    # print('writing {} {}-{} {}:{}'.format(variable_selection[v_cnt], counter_idxs[0], counter_idxs[-1],
                    #                                       data_strt[v_cnt, counter_idxs[0]],
                    #                                       data_end[v_cnt, counter_idxs[0]]))
                    out[v_cnt, counter_idxs, data_strt[v_cnt, counter_idxs[0]]:data_end[v_cnt, counter_idxs[0]]] = dat
                    if v_cnt == 0:
                        out_sec[v_cnt, counter_idxs, data_strt[v_cnt, counter_idxs[0]]:data_end[v_cnt, counter_idxs[0]]] = sec
                    data_strt[v_cnt, counter_idxs] = data_end[v_cnt, counter_idxs]

                    if (len(np.unique(data_strt[v_cnt, counter_idxs])) > 1) or (
                            len(np.unique(data_end[v_cnt, counter_idxs])) > 1):
                        raise NotImplementedError('Mixing of ping counter numbers across sector not supported currently')

        if np.any(out):
            final_idx = ~np.isnan(out)
            finalout = out[final_idx]
            try:
                finalout = finalout.reshape(expected_shape)
            except ValueError:
                revised_pingcount = int(len(finalout)/expected_shape[0]/expected_shape[2])
                print('Did not get the expected number of pings, returning {} pings, expected '.format(revised_pingcount) +
                      '{} given the number of ping counter indices for the time provided'.format(expected_shape[1]))
                expected_shape = (expected_shape[0], revised_pingcount, expected_shape[2])
                expected_sec_shape = (expected_sec_shape[0], revised_pingcount, expected_sec_shape[2])
                finalout = finalout.reshape(expected_shape)
            finalsec = out_sec[np.expand_dims(final_idx[0], axis=0)].reshape(expected_sec_shape)
            return finalout, finalsec
        else:
            print('Unable to find records for {} for time {}'.format(variable_selection, tme))
            return None, None

    def return_times_across_sectors(self):
        """
        Return all the times that are within at least one sector in the fqpr dataset.  If a time shows up twice
        (EM2040c will do this, with two sectors and no delay between them), return the array sorted so that the time
        shows up twice in the returned array as well.

        Returns
        -------
        numpy array, 1d array of timestamps

        """
        tims = np.concatenate([self.fqpr[sec].time.values for sec in self.fqpr])
        tims.sort()
        return tims

    def return_unique_times_across_sectors(self):
        """
        Return all the unique times that are within at least one sector in the fqpr dataset

        Returns
        -------
        numpy array, 1d array of timestamps

        """
        return np.unique(np.concatenate([self.fqpr[sec].time.values for sec in self.fqpr]))

    def calc_min_var(self, varname='depth_offset'):
        """
        For given variable, return the minimum value found across all sectors

        Parameters
        ----------
        varname: string, name of the variable you are interested in

        Returns
        -------
        float, minimum value across all sectors

        """
        mins = np.array([])
        for sec in self.fqpr:
            mins = np.append(mins, float(self.fqpr[sec][varname].min()))
        return mins.min()

    def calc_max_var(self, varname='depth_offset'):
        """
        For given variable, return the maximum value found across all sectors

        Parameters
        ----------
        varname: string, name of the variable you are interested in

        Returns
        -------
        float, maximum value across all sectors

        """
        maxs = np.array([])
        for sec in self.fqpr:
            maxs = np.append(maxs, float(self.fqpr[sec][varname].max()))
        return maxs.max()


def flatten_bool_xarray(datarray, cond, retain_dim='time', drop_var=None):
    """
    Takes in a two dimensional DataArray with core dimension 'time' and a second dimension that has only one valid
    value according to provided cond.  Outputs DataArray with those values and either only the 'time' dimension
    (drop_var is the dimension name to drop) or with both dimensions intact.

    tst.raw_rangeangle.tiltangle
    Out[11]:
    <xarray.DataArray 'tiltangle' (time: 7836, sector: 16)>
    dask.array<zarr, shape=(7836, 16), dtype=float64, chunksize=(5000, 16), chunktype=numpy.ndarray>
    Coordinates:
      * sector   (sector) <U12 '218_0_070000' '218_0_071000' ... '218_2_090000'
      * time     (time) float64 1.474e+09 1.474e+09 ... 1.474e+09 1.474e+09

    tst.raw_rangeangle.tiltangle.isel(time=0).values
    Out[6]:
    array([ 0.  ,  0.  ,  0.  ,  0.  ,  0.  ,  0.  ,  0.  , -0.38,  0.  ,
            0.  ,  0.  ,  0.  ,  0.  ,  0.  ,  0.  ,  0.  ])

    answer = fqpr_generation.flatten_bool_xarray(tst.raw_rangeangle.tiltangle, tst.raw_rangeangle.ntx > 0,
                                                 drop_var='sector')
    answer
    Out[10]:
    <xarray.DataArray 'tiltangle' (time: 7836)>
    dask.array<vindex-merge, shape=(7836,), dtype=float64, chunksize=(7836,), chunktype=numpy.ndarray>
    Coordinates:
        * time     (time) float64 1.474e+09 1.474e+09 ... 1.474e+09 1.474e+09

    answer.isel(time=0).values
    Out[9]: array(-0.38)

    Parameters
    ----------
    datarray: xarray DataArray, 2 dimensional
    cond: xarray DataArray, boolean mask for datarray, see example above
    retain_dim: str, core dimension of datarray
    drop_var: str, dimension to drop, optional

    Returns
    -------

    """
    datarray = datarray.where(cond)
    if datarray.ndim == 2:
        # True/False where non-NaN values are
        true_idx = np.argmax(datarray.notnull().values, axis=1)
        data_idx = xr.DataArray(true_idx, coords={retain_dim: datarray.time}, dims=[retain_dim])
    else:
        raise ValueError('Only 2 dimensional DataArray objects are supported')

    answer = datarray[0:len(datarray.time), data_idx]
    if drop_var:
        answer = answer.drop_vars(drop_var)
    return answer


def cross(a, b, spatial_dim, output_dtype=None):
    """
    Xarray-compatible cross product.  Compatible with dask, parallelization uses a.dtype as output_dtype

    Parameters
    ----------
    a: xarray DataArray
    b: xarray DataArray
    spatial_dim: str, dimension name to be mulitplied through
    output_dtype: numpy dtype, dtype of output

    Returns
    -------
    c: xarray DataArray, cross product of a and b along spatial_dim

    """
    for d in (a, b):
        if spatial_dim not in d.dims:
            raise ValueError('dimension {} not in {}'.format(spatial_dim, d))
        if d.sizes[spatial_dim] != 3:
            raise ValueError('dimension {} has not length 3 in {}'.format(spatial_dim, d))

    if output_dtype is None:
        output_dtype = a.dtype
    c = xr.apply_ufunc(np.cross, a, b,
                       input_core_dims=[[spatial_dim], [spatial_dim]],
                       output_core_dims=[[spatial_dim]],
                       dask='parallelized', output_dtypes=[output_dtype]
                       )
    return c


def generate_new_xyz(outputfolder,
                     filname=r"C:\collab\dasktest\data_dir\EM2040_smallfil\0009_20170523_181119_FA2806.all"):
    mbes_read = BatchRead(filname)
    fq = Fqpr(mbes_read)
    fq.read_from_source()
    fq.get_orientation_vectors()
    fq.build_beam_pointing_vector()
    fq.sv_correct()
    fq.georef_across_along_depth(outfold=outputfolder, ellip='GRS80', vert_ref='ellipse')
