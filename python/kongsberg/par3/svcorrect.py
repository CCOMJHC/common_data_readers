import os, sys
import numpy as np
import json
from collections import OrderedDict
import xarray as xr

from UTC import julian_day_time_to_utctimestamp
from dms import parse_dms_to_dd
from rotations import build_rot_mat
from xarray_helpers import stack_nan_array, reform_nan_array

supported_file_formats = ['.svp']


class SoundSpeedProfile:
    """
    Profile datastore, can accept inputs from:
    - prof_type = raw_rangeangle, soundspeed cast comes in from xarray Dataset attribute, used with xarray_conversion
    - prof_type = caris_svp, soundspeed cast comes in as file path to caris .svp file

    ex: raw_rangeangle

    tst.raw_rangeangle[0].profile_1495599960
    Out[7]: '[[0.0, 1489.2000732421875], [0.32, 1489.2000732421875], [0.5, 1488.7000732421875], ...
    cst = svcorrect.SoundSpeedProfile(tst.raw_rangeangle[0].profile_1495599960, prof_time=1495599960,
                                      prof_name='profile_1495599960', prof_type='raw_rangeangle')
    cst.profile
    Out[9]:  {0.0: 1489.2000732421875, 0.32: 1489.2000732421875, 0.5: 1488.7000732421875, ...

    ex: caris_svp

    cst = svcorrect.SoundSpeedProfile(r'C:\\Users\\eyou1\\Downloads\\2016_288_021224.svp', prof_type='caris_svp')
    cst.prof_time
    Out[14]: 1476411120.0
    cst.prof_name
    Out[15]: '2016_288_021224.svp'
    cst.prof_location
    Out[16]: [37.58972222222222, -76.10972222222222]
    cst.profile
    Out[17]: {1.2: 1505.25, 1.6: 1505.26, 1.81: 1505.27, 2.18: 1505.29, 4.42: 1506.05, 5.38: 1507.08, ...

    Will read from input data and generate profile dict for looking up soundspeed at depth, as well as other cast
    metadata
    """

    def __init__(self, raw_profile, z_val, ss_sound_speed, prof_name=None, prof_time=None, prof_location=None, prof_type='raw_rangeangle'):
        self.raw_profile = raw_profile
        self.prof_type = prof_type
        self.prof_time = prof_time
        self.prof_name = prof_name
        self.prof_location = prof_location
        self.profile = self.load_from_profile()

        self.z_val = -z_val
        self.ss_sound_speed = ss_sound_speed
        self.corr_profile = None
        self.corr_profile_lkup = None
        self.adjust_for_z()

        self.interpolate_extended_casts()

        self.lkup_across_dist = None
        self.lkup_down_dist = None
        self.dim_raytime = None
        self.dim_angle = None

    def __sizeof__(self):
        """
        Calculate size of the large attributes of the class and return total size in bytes

        Returns
        -------
        int, total size of large attributes in bytes

        """
        soundspeed_size = sys.getsizeof(np.array(self.ss_sound_speed.values))
        prof_size = sys.getsizeof(self.raw_profile) + sys.getsizeof(self.profile)
        corr_prof_size = sys.getsizeof(self.corr_profile_lkup) + sys.getsizeof(self.corr_profile)
        lkup_size = sys.getsizeof(self.lkup_across_dist) + sys.getsizeof(self.lkup_down_dist)
        dim_size = sys.getsizeof(self.dim_raytime) + sys.getsizeof(self.dim_angle)
        return soundspeed_size + prof_size + corr_prof_size + lkup_size + dim_size

    def load_from_profile(self):
        """
        Uses prof_type to run correct load function, returns cast data

        Returns
        -------
        dict, keys are depth and values are soundspeed
        """
        if self.prof_type == 'raw_rangeangle':  # profile comes from xarray_conversion raw_rangeangle dataset attribute
            return self.load_from_xarray()
        elif self.prof_type == 'caris_svp':
            return self.load_from_caris_svp()
        else:
            raise ValueError('Unrecognized format: {}'.format(self.prof_type))

    def load_from_xarray(self):
        """
        Xarray dataset from xarray_conversion BatchRead class stores profile in json so it can be serialized.  We want
        a dictionary where we can look up the depth and get a soundspeed.  Convert it here.

        Returns
        -------
        profdata: dict, keys are depth in meters and values are soundspeed in m/s
        """
        if type(self.raw_profile) == str:   # this is how it should come in, as a json string
            self.raw_profile = json.loads(self.raw_profile)

        # insert zero point in profile with matching soundspeed as first actual layer, for when trans is above the
        #   shallowest point in the cast
        self.raw_profile = [['0.0', self.raw_profile[0][1]]] + self.raw_profile

        profdata = {float(k): float(v) for k, v in self.raw_profile}
        return OrderedDict(sorted(profdata.items()))

    def _load_caris_svp_data(self, svpdata):
        """
        See load_from_caris_svp, takes in readlines output from svp file, returns dict

        Parameters
        ----------
        svpdata: list, output from readlines call on svp file

        Returns
        -------
        profdata: dict, keys are depth in meters and values are soundspeed in m/s
        """
        svpdat = [[float(dproc) for dproc in svp.rstrip().split()] for svp in svpdata]
        # insert zero point in profile with matching soundspeed as first actual layer, for when trans is above the
        #   shallowest point in the cast
        svpdat = [[0.0, svpdat[0][1]]] + svpdat

        profdata = {k: v for k, v in svpdat}
        return OrderedDict(sorted(profdata.items()))

    def load_from_caris_svp(self):
        """
        Here we load from a caris svp file to generate the cast data.  We want a dictionary where we can look up the
        depth and get a soundspeed.

        Returns
        -------
        profdata: dict, keys are depth in meters and values are soundspeed in m/s
        """
        if os.path.exists(self.raw_profile) and self.raw_profile.endswith('.svp'):
            with open(self.raw_profile, 'r') as svp:
                version = svp.readline()  # [SVP_VERSION_2]
                name = svp.readline()  # 2016_288_021224.svp
                header = svp.readline()  # Section 2016-288 02:12 37:35:23 -076:06:35
                svpdata = svp.readlines()
                try:
                    jdate, tme, lat, lon = [header.split()[i] for i in [1, 2, 3, 4]]
                except IndexError:
                    error = 'Error reading {}\n'.format(self.raw_profile)
                    error += 'Please verify that the svp file has the correct header'
                    raise IOError(error)

                jdate = jdate.split('-')
                tme = tme.split(':')
                if self.prof_location is None:
                    self.prof_location = [parse_dms_to_dd(lat), parse_dms_to_dd(lon)]
                if self.prof_time is None:
                    if len(tme) == 3:
                        self.prof_time = julian_day_time_to_utctimestamp(int(jdate[0]), int(jdate[1]), int(tme[0]),
                                                                         int(tme[1]), int(tme[2]))
                    elif len(tme) == 2:
                        self.prof_time = julian_day_time_to_utctimestamp(int(jdate[0]), int(jdate[1]), int(tme[0]),
                                                                         int(tme[1]), 0)
                    else:
                        raise ValueError('Unrecognized timestamp: {}'.format(tme))
                if self.prof_name is None:
                    self.prof_name = os.path.split(name.rstrip())[1]
                return self._load_caris_svp_data(svpdata)
        else:
            raise IOError('Not a valid caris svp file: {}'.format(self.raw_profile))

    def adjust_for_z(self):
        """
        self.profile contains the sound velocity data as seen by the profiler.  We need a table that starts at the depth
        of the sonar relative to the waterline.  Also need to insert a snapback soundspeed layer equal to the data seen
        by the surface sound velocimeter.  This method will generate a list of lookup tables that equal in length to the
        length of the unique surface soundvelocity entries.

        Returns
        -------

        """
        unique_ss = sorted(np.unique(self.ss_sound_speed))
        self.corr_profile_lkup = np.searchsorted(unique_ss, self.ss_sound_speed)
        self.corr_profile = []
        print('Generating lookup tables for {} unique soundspeed entries...'.format(len(unique_ss)))
        for u in unique_ss:
            newprof = OrderedDict({i - self.z_val: j for (i, j) in self.profile.items() if i - self.z_val >= 0})
            frst_ss = OrderedDict({0: u})
            self.corr_profile.append(OrderedDict(list(frst_ss.items()) + list(newprof.items())))

    def interpolate_extended_casts(self, max_allowable_depth_distance=100):
        """
        Take max distance parameter, interpolate layers with depth differences greater than that.  This is super
        important as extending a cast from 100m to 1200m to satisfy Kongsberg might result in a change in tens of m/s
        between layers.  This results in steering angles with a huge change across that boundary.

        Parameters
        ----------
        max_allowable_depth_distance: int, max allowable distance in meters between layer entries.

        Returns
        -------
        self.profile: OrderedDict, profile with new interpolated layers wherever necessary

        """
        rslt = []
        for prof in self.corr_profile:
            dpths = np.array(list(prof))
            dif_dpths = np.diff(dpths)
            needs_interp = dif_dpths > max_allowable_depth_distance
            if np.any(needs_interp):
                interp_idx = np.where(needs_interp)
                if len(interp_idx) > 1:
                    raise ValueError(
                        'Found more than one gap in profile greater than {}'.format(max_allowable_depth_distance))
                firstval = dpths[interp_idx[0]][0]
                secval = dpths[interp_idx[0] + 1][0]
                new_dpths = np.round(np.linspace(firstval + max_allowable_depth_distance, secval,
                                                (secval - firstval) / max_allowable_depth_distance), 2)
                new_svp = np.interp(new_dpths, [firstval, secval], [prof[firstval], prof[secval]])
                for d, s in zip(new_dpths, new_svp):
                    prof[d] = s
            rslt.append(OrderedDict(sorted(prof.items())))
        self.corr_profile = rslt

    def generate_lookup_table(self, max_pointing_angle=90.0, beam_inc=0.02):
        """
        Compute a lookup table for all possible launch angles to get acrosstrack/alongtrack distance and travel time.
        Build look up table around approximate launch angles, something like .02 deg increments.  When using table, find
        nearest launch angle that applies.  Error should be within unc of the attitude sensor (plus patch uncertainty)
        Table dims look something like 70 * 50 (launch angle * beam increment) by 50 (sound speed layers)
        Table is indexed by time.  Knowing two-way-travel-time, search table for final x, y.
        Table would be from the waterline, when using it in practice, have to offset by the transducer vertical position
        relative to the waterline.

        Parameters
        ----------
        max_pointing_angle: float, max angle of the swath
        beam_inc: float, beam angle increments you want to generate entries for

        Returns
        -------
        self.lookup_table: xarray Dataset, generated lookup table allowing for searching by angle/layer

        """
        # max_pointing_angle is from boresight, only doing one side as port/stbd are the same
        print('Generating lookup table to {} deg in {} deg increments'.format(max_pointing_angle, beam_inc))
        starter_angles = np.arange(np.deg2rad(0), np.deg2rad(max_pointing_angle), np.deg2rad(beam_inc))

        lookup_table_angles = np.zeros([len(self.corr_profile), starter_angles.shape[0], len(self.corr_profile[0])])
        cumulative_ray_time = np.zeros([len(self.corr_profile), starter_angles.shape[0], len(self.corr_profile[0])])
        cumulative_across_dist = np.zeros([len(self.corr_profile), starter_angles.shape[0], len(self.corr_profile[0])])
        cumulative_down_dist = np.zeros([len(self.corr_profile), len(self.corr_profile[0])])

        for pcnt, prof in enumerate(self.corr_profile):
            lookup_table_angles[pcnt, :, 0] = starter_angles
            dpths = np.array(list(prof.keys()))
            print('- Lookup table to depth of {} with {} total layers...'.format(np.max(dpths), len(dpths)))
            for cnt, dpth in enumerate(dpths):
                if cnt != len(dpths) - 1:
                    # ray parameters for all lookup angles
                    difdpth = dpths[cnt + 1] - dpth
                    across_dist = difdpth * np.tan(lookup_table_angles[pcnt, :, cnt])
                    ray_dist = np.sqrt(difdpth**2 + across_dist**2)
                    ray_time = ray_dist / prof[dpth]

                    cumulative_ray_time[pcnt, :, cnt + 1] = cumulative_ray_time[pcnt, :, cnt] + ray_time
                    cumulative_across_dist[pcnt, :, cnt + 1] = cumulative_across_dist[pcnt, :, cnt] + across_dist
                    cumulative_down_dist[pcnt, cnt + 1] = cumulative_down_dist[pcnt, cnt] + difdpth

                    # incidence angles for next layer
                    # use clip to clamp values where beams are reflected, i.e. greater than 1 (90°)
                    #  - this happens with a lot of the extended-to-1200m casts in kongsberg data, these angles should
                    #    not be used, this is mostly to suppress the runtime warning
                    _tmpdat = prof[dpths[cnt + 1]] / prof[dpths[cnt]] * np.sin(lookup_table_angles[pcnt, :, cnt])
                    next_angles = np.arcsin(np.clip(_tmpdat, -1, 1))
                    lookup_table_angles[pcnt, :, cnt + 1] = next_angles
        self.lkup_across_dist = cumulative_across_dist
        self.lkup_down_dist = cumulative_down_dist
        self.dim_raytime = cumulative_ray_time
        self.dim_angle = starter_angles

    def _interp(self, arr, row_idx, col_idx, msk, interp_factor):
        """
        See return_interp_beam_xy.  Takes in an array (arr) and some parameters that allow you to interp to the given
        interp factor.  Interp_factor represents location of actual values you are wanting, a ratio you can use to find
        those values.  Using this instead of numpy/scipy/xarray methods as I couldn't find anything that really handles
        this odd 2d indexing that I'm doing.  Probably because I don't know what I'm doing.

        EX: looking at first val in vectors

        In:  row_idx[0]
        Out: 0
        In:  col_idx[0]
        Out: 35
        In:  msk[0]
        Out: 0
        In:  arr[row_idx[0], col_idx[0]]
        Out: 74.2
        In:  arr[row_idx[0], col_idx[0] + 1]
        Out: 174.2
        ('0' is in msk, so we know to use the previous value instead, the desired value is before 74.2)
        In:  arr[row_idx[0], col_idx[0] - 1]
        Out: 60.67
        In:  interp_factor[0]
        Out: 0.28855453
        (actual value is this factor times diff away from start value)
        (74.2 - (0.28855453 * (74.2 - 60.67)))
        In:  np.round(start - (interp_factor * (start - end)), 2)
        Out: 70.30

        Parameters
        ----------
        arr: numpy nDarray, 2d array that you are wanting an interpolated vector from
        row_idx: numpy array, 1d array of index values for 1st dimension
        col_idx: numpy array, 1d array of index values for 2nd dimension
        msk: numpy array, 1d array of index values that mark when you want to interp using the value before the nearest
             value instead of ahead
        interp_factor: numpy ndarray, 1d array of values that are the interp ratio to apply to achieve linear
                       interpolation between the start/end values

        Returns
        -------
        interp_vals: numpy ndarray,

        """
        # use the mask and interpolation factor generated above to build across/alongtrack offsets
        start = arr[row_idx, col_idx]
        end = arr[row_idx, col_idx + 1]
        end[msk] = arr[row_idx, col_idx - 1][msk]
        interp_vals = np.round(start - (interp_factor * (start - end)), 3)  # round to mm
        return interp_vals

    def _rotate_2d_to_3d(self, x, y, angle, data_idx):
        """
        Take vector with x/y (2d) components and then rotate it along x axis to get 3d vector.

        In:  angle.values[0]
        Out: -0.6200000047683716
        In:  zerod_x.values[0]
        Out: 0
        In:  zerod_y.values[0]
        Out: 0
        In:  rotmat.values[0]
        Out: array([[ 1.        , -0.        ,  0.        ],
                    [ 0.        ,  0.99994145,  0.01082083],
                    [-0.        , -0.01082083,  0.99994145]])
        (use data_idx values to replicate rotation matrix for each matching time value, have to do this for the
        flattened time/beam arrays we are working with)
        In:  expanded_rotmat = rotmat[data_idx].values
        In:  vec = np.squeeze(np.dstack([x, y, np.zeros_like(x)]))
        In:  vec[0]
        Out: array([239.71445634,  70.29585725,   0.        ])
        In:  rotvec = np.einsum('lij,lj->li', expanded_rotmat, vec)
        In:  rotvec[0]
        Out: array([239.71445634,  70.29174165,  -0.76065954])

        Parameters
        ----------
        x: xarray DataArray, 1d stacked array of acrosstrack offsets with MultiIndex coords of time/beam
        y: xarray DataArray, 1d stacked array of alongtrack offsets with MultiIndex coords of time/beam
        angle: xarray DataArray, 1d array with coordinate 'time', represents the x axis angle change over time
               data_idx
        data_idx: numpy array 1d, with length of stacked array, contains the index of the appropriate rotation matrix
                  to use for that vector.  EX: array([0,0,0,1,1,1]) says to use the first rotmat for the first three
                  vectors and the second rotmat for the last three vectors

        Returns
        -------
        newacross: numpy array 1d, rotated acrosstrack offset
        newdown: numpy array 1d, newly generated down offset, from rotating xy
        newalong: numpy array 1d, rotated alongtrack offset

        """
        zerod_x = xr.DataArray(np.zeros_like(angle), coords={'time': angle.time}, dims=['time'])
        zerod_y = zerod_x
        rotmat = build_rot_mat(angle, zerod_x, zerod_y)
        expanded_rotmat = rotmat[data_idx].values

        vec = np.squeeze(np.dstack([x, y, np.zeros_like(x)]))
        rotvec = np.round(np.einsum('lij,lj->li', expanded_rotmat, vec), 3)  # round to mm

        newacross = rotvec[:, 0]
        newdown = rotvec[:, 1]
        newalong = rotvec[:, 2]

        return newacross, newdown, newalong

    def _construct_across_down_vals(self, nearest_raytimes, nearest_across, nearest_down, one_way_travel_time):
        """
        Using SoundSpeedProfile as a lookup table, find the nearest raytime to the given one_way_travel_time, determine
        the proportional difference, and apply that same factor to across track/depth to get the interpolated xy at
        that time.

        Parameters
        ----------
        nearest_raytimes: numpy array, first dim is beams, second is cumulative ray time for water column
        nearest_across: numpy array, first dim is beams, second is cumulative across track distance for water column
        nearest_down: numpy array, first dim is beams, second is cumulative depth for water column
        one_way_travel_time: numpy array, one dimensional array of the one way travel time for each beam

        Returns
        -------
        interp_acrossvals: numpy array, one dimensional array of across track distance for each beam
        interp_downvals: numpy array, one dimensional array of depth for each beam

        """
        # get the index of the closest onewaytraveltime from nearest raytimes
        # self.dim_raytime will have nan for part of profile where beams reflect, angle>90, use nanargmin
        srch_vals = np.nanargmin(np.abs(nearest_raytimes - np.expand_dims(one_way_travel_time, axis=1)), axis=1)
        srch_rows = np.arange(len(nearest_raytimes))

        # get val before and after actual value nearest the index
        raytim1 = nearest_raytimes[srch_rows, srch_vals]
        raytim2 = nearest_raytimes[srch_rows, srch_vals + 1]
        # sometimes the bound is the other way, when the nearest is greater than the index
        msk = np.where(one_way_travel_time < raytim1)[0]
        raytim2[msk] = nearest_raytimes[srch_rows, srch_vals - 1][msk]

        # get the interp factor to apply to other arrays
        fac = (raytim1 - one_way_travel_time) / (raytim1 - raytim2)
        interp_acrossvals = self._interp(nearest_across, srch_rows, srch_vals, msk, fac)
        interp_downvals = self._interp(nearest_down, srch_rows, srch_vals, msk, fac)
        return interp_acrossvals, interp_downvals

    def _transducer_depth_correction(self, transdepth, nearest_across):
        """
        OUTDATED: NOT IN USE.  REPLACED BY ADDING SSV LAYER AND ADJUSTING PROFILE FOR TRANS DEPTH REL WATERLINE

        Take in a floating point number for height of waterline from transducer and generate a time series corrector
        for acrosstrack offsets.

        Parameters
        ----------
        transdepth: float, height of waterline from transducer, positive down (i.e.
                       transdepth=-0.640 means the waterline is 64cm above the transducer)
        nearest_across: numpy array, first dim is beams, second is cumulative across track distance for water column

        Returns
        -------
        interp_across: numpy array, first dim is beams, second is a corrector for across track distance taking into
                       account the transducer position relative to the waterline

        """
        # here we want to use transdepth in our table, so we make it positive
        transdepth = np.abs(transdepth)

        dpths = np.array(list(self.profile.keys()))

        # get the index of the closest depth from transdepth
        srch_vals = np.argmin(np.abs(dpths - transdepth))
        srch_rows = np.arange(len(nearest_across))

        # get val before and after actual value nearest the index
        dpth1 = dpths[srch_vals]
        if transdepth > dpth1:
            offset = 1
        else:
            offset = -1
        dpth2 = dpths[srch_vals + offset]

        # get the interp factor to apply to other arrays
        fac = (dpth1 - transdepth) / (dpth1 - dpth2)

        # apply interp factor to get across value corrector
        interp_across = np.round(nearest_across[srch_rows, srch_vals] - (fac * (nearest_across[srch_rows, srch_vals] -
                                 nearest_across[srch_rows, srch_vals + offset])), 3)

        return interp_across

    def return_interp_beam_xy(self, beam_angle, two_way_travel_time, beam_azimuth, subset=None):
        """
        To better understand inputs, see fqpr_generation.FQPR.build_beam_pointing_vector

        Use the generated lookup table to return across track / along track offsets from given beam angles and travel
        times.  We interpolate the table to get the values at the actual given travel time.  For beam angle, we simply
        search for the nearest value, assuming the table beam angle increments are roughly on par with the accuracy
        of the attitude sensor.

        Parameters
        ----------
        beam_angle: Xarray DataArray,  2 dimension array of time/beam angle.  Assumes the second dim contains the
                    actual angles, ex: (time, angle)
        two_way_travel_time: Xarray DataArray, 2 dimension array of time/two_way_travel_time.  Assumes the second dim
                    contains the actual traveltime, ex: (time, twtt)
        beam_azimuth: Xarray DataArray, 2 dimension array of time/beam azimuth.  Assumes the second dim contains the
                     actual beam azimuth data, ex: (time, azimuth)
        subset: numpy array, if provided subsets the corrected profile lookup

        Returns
        -------
        reformed_alongvals: xarray DataArray, (time, along track offset in meters)
        reformed_acrossvals: xarray DataArray, (time, across track offset in meters)
        reformed_downvals: xarray DataArray, (time, down distance in meters)

        """
        if self.lkup_across_dist is None:
            raise ValueError('Generate lookup table first')
        if subset is not None:
            corr_lkup = self.corr_profile_lkup[subset]
        else:
            corr_lkup = self.corr_profile_lkup

        # take in (time, beam) dimension data or (beam) dimension data and flatten so we can work on what are likely
        #   jagged arrays with np.nan padding.  Maintain original shape for reshaping after
        # use stack to get a 1d multidimensional index, allows you to do the bool indexing later to remove nan vals
        # - use absvalue of beam_angle_stck for the lookup table, retain original beam_angle to determine port/stbd
        orig_shape = beam_angle.shape
        orig_coords = beam_angle.coords
        orig_dims = beam_angle.dims
        beam_idx, beam_angle_stck = stack_nan_array(beam_angle, stack_dims=('time', 'beam'))
        twtt_idx, twoway_stck = stack_nan_array(two_way_travel_time, stack_dims=('time', 'beam'))
        beamaz_idx, beamaz_stck = stack_nan_array(beam_azimuth, stack_dims=('time', 'beam'))
        del beam_angle, two_way_travel_time, beam_azimuth

        beam_angle_stck = np.abs(beam_angle_stck)
        oneway_stck = twoway_stck / 2
        del twoway_stck

        # get indexes of nearest beam_angle to the table angle dimension, default is first suitable location (left)
        # TODO: this gets the index after (insertion index), look at getting nearest index
        nearest_angles = np.searchsorted(self.dim_angle, beam_angle_stck)
        del beam_angle_stck

        # get the arrays according to the nearest angle search
        # lkup gives the right table to use for each run, multiple tables exist for each unique ssv entry
        lkup = corr_lkup[beam_idx[0]]
        nearest_raytimes = self.dim_raytime[lkup, nearest_angles, :]
        nearest_across = self.lkup_across_dist[lkup, nearest_angles, :]
        nearest_down = self.lkup_down_dist[lkup]

        print('Running sv_correct on {} total beams...'.format(nearest_raytimes.shape[0]))

        interp_acrossvals, interp_downvals = self._construct_across_down_vals(nearest_raytimes, nearest_across,
                                                                              nearest_down, oneway_stck)
        del nearest_raytimes, nearest_across, nearest_down, oneway_stck

        # here we use the beam azimuth to go from xy sv corrected beams to xyz soundings
        newacross = interp_acrossvals * np.sin(beamaz_stck)
        newalong = interp_acrossvals * np.cos(beamaz_stck)
        del interp_acrossvals

        reformed_across = reform_nan_array(newacross, beam_idx, orig_shape, orig_coords, orig_dims)
        reformed_downvals = reform_nan_array(interp_downvals, beam_idx, orig_shape, orig_coords, orig_dims)
        reformed_along = reform_nan_array(newalong, beam_idx, orig_shape, orig_coords, orig_dims)
        del newacross, newalong, interp_downvals

        return np.round(reformed_along, 3), np.round(reformed_across, 3), np.round(reformed_downvals, 3)


def get_sv_files_from_directory(dir_path, search_subdirs=True):
    """
    Returns a list of all files that have an extension in the global variable supported_file_formats

    Disable search_subdirs if you want to only search the given folder, not subfolders

    Parameters
    ----------
    dir_path: string, path to the parent directory containing sv files
    search_subdirs: bool, if True searches all subfolders as well

    Returns
    -------
    svfils: list, full file paths to all sv files with approved file extension

    """
    svfils = []
    for root, dirs, files in os.walk(dir_path):
        for f in files:
            if os.path.splitext(f)[1] in supported_file_formats:
                svfils.append(os.path.join(root, f))
        if not search_subdirs:
            break
    return svfils


def return_supported_casts_from_list(list_files):
    """
    Take in a list of files, return all the valid cast files

    Parameters
    ----------
    list_files: list, list of files

    Returns
    -------
    files from list that have extension in supported_file_formats

    """
    if type(list_files) != list:
        raise TypeError('Function expects a list of sv files to be provided.')
    return [x for x in list_files if os.path.splitext(x)[1] in supported_file_formats]


def run_ray_trace(cast, orig_idx, beam_angle, two_way_travel_time, beam_azimuth):
    """
    Convenience function for running svcorrect on provided data.

    Sources:
       *Ray Trace Modeling of Underwater Sound Propagation - Jens M. Hovern
       medwin/clay, fundamentals of acoustical oceanography, ch3
       Underwater Ray Tracing Tutorial in 2D Axisymmetric Geometry - COMSOL
       Barry Gallagher, hydrographic wizard extraordinaire

    Ray Theory approach - assumes sound propagates along rays normal to wave fronts, refracts when sound speed changes
    in the water.  Layers of differing sound speed are obtained through sound speed profiler.  Here we make linear
    approximation, where we assume sound speed changes linearly between layers.

    When searching by half two-way-travel-time, unlikely to find exact time in table.  Find nearest previous time,
    get the difference in time and multiply by the layer speed to get distance.

    Parameters
    ----------
    cast: SoundSpeedProfile, cast to use in raytracing
    orig_idx: numpy array, subset of cast needed to trim the corrected profile lookup.  corrected profile lookup is
              based on the total length of the provided surface sound speed
    beam_angle: Xarray DataArray,  2 dimension array of time/beam angle.  Assumes the second dim contains the
                actual angles, ex: (time, angle)
    two_way_travel_time: Xarray DataArray, 2 dimension array of time/two_way_travel_time.  Assumes the second dim
                         contains the actual traveltime, ex: (time, twtt)
    beam_azimuth: Xarray DataArray, 2 dimension array of time/beam azimuth.  Assumes the second dim contains the
                     actual beam azimuth data, ex: (time, azimuth)

    Returns
    -------
    x: xarray DataArray, (time, along track offset in meters)
    y: xarray DataArray, (time, across track offset in meters)
    z: xarray DataArray, (time, down distance in meters)

    """
    x, y, z = cast.return_interp_beam_xy(beam_angle, two_way_travel_time, beam_azimuth, subset=orig_idx)
    return x, y, z


def distributed_run_sv_correct(worker_dat):
    x, y, z = run_ray_trace(worker_dat[0], worker_dat[1], worker_dat[2], worker_dat[3], worker_dat[4])
    return x, y, z
