import xarray as xr
import numpy as np
from dask.array import einsum


def build_rot_mat(roll, pitch, yaw, order='rpy', degrees=True):
    """
    Make the rotation matrix for a set of angles and return the matrix.
    All file angles are in degrees, so incoming angles are degrees.

    Intrinsic - each rotation performed on coordinate system as rotated by previous operation
    Intrinsic rotation, rot(rpy) = rot(y)*rot(p)*rot(r)

    Roll pitch yaw assumed to be xarray DataArray, else will try single values

    Parameters
    ----------
    roll: numpy/xarray, array of floating point numbers
    pitch: numpy/xarray, array of floating point numbers
    yaw: numpy/xarray, array of floating point numbers
    order: str, order of rotation
    degrees: bool, True if incoming angles are in degrees, False if radians

    Returns
    -------
    rmat: xarray DataArray, rotation matrix composed of rpy rotations

    """
    if type(roll) != xr.DataArray or type(pitch) != xr.DataArray or type(yaw) != xr.DataArray:
        raise TypeError('Expected xarray DataArray object')

    if order == 'ypr':
        r = yaw
        p = pitch
        y = roll
    elif order == 'rpy':
        r = roll
        p = pitch
        y = yaw
    else:
        raise ValueError('Order provided is not rpy or ypr.')

    if degrees:
        r = np.deg2rad(r)
        p = np.deg2rad(p)
        y = np.deg2rad(y)

    rcos = np.cos(r)
    pcos = np.cos(p)
    ycos = np.cos(y)
    rsin = np.sin(r)
    psin = np.sin(p)
    ysin = np.sin(y)

    r00 = (ycos * pcos).assign_coords({'x': 0, 'y': 0})
    r01 = (ycos * psin * rsin - ysin * rcos).assign_coords({'x': 0, 'y': 1})
    r02 = (ycos * psin * rcos + ysin * rsin).assign_coords({'x': 0, 'y': 2})
    r0 = xr.concat([r00, r01, r02], dim='y')
    r10 = (ysin * pcos).assign_coords({'x': 1, 'y': 0})
    r11 = (ysin * psin * rsin + ycos * rcos).assign_coords({'x': 1, 'y': 1})
    r12 = (ysin * psin * rcos - ycos * rsin).assign_coords({'x': 1, 'y': 2})
    r1 = xr.concat([r10, r11, r12], dim='y')
    r20 = (-psin).assign_coords({'x': 2, 'y': 0})
    r21 = (pcos * rsin).assign_coords({'x': 2, 'y': 1})
    r22 = (pcos * rcos).assign_coords({'x': 2, 'y': 2})
    r2 = xr.concat([r20, r21, r22], dim='y')

    rmat = xr.concat([r0, r1, r2], dim='x').transpose('time', 'x', 'y')

    return rmat


def build_mounting_angle_mat(roll, pitch, yaw, tstmp):
    """
    Feeds build_rot_mat, difference being this takes in single floating point numbers for rpy as you get from a
    surveyed mount angle data point.

    Assumes angles are in degrees and rpy rotation is desired.

    Parameters
    ----------
    roll: float, roll angle for rotation matrix
    pitch: float, pitch angle for rotation matrix
    yaw: float, yaw angle for rotation matrix
    tstmp: string, time relevant installation parameter showed up in the multibeam file

    Returns
    -------
    rmat: xarray DataArray, rotation matrix composed of rpy rotations

    """
    if type(roll) != float or type(pitch) != float or type(yaw) != float:
        raise TypeError('Expected floating point values for roll,pitch,yaw')

    time_coord = np.array([float(tstmp)])
    roll_xarr = xr.DataArray(np.array([roll]), dims=['time'], coords={'time': time_coord}).chunk()
    pitch_xarr = xr.DataArray(np.array([pitch]), dims=['time'], coords={'time': time_coord}).chunk()
    yaw_xarr = xr.DataArray(np.array([yaw]), dims=['time'], coords={'time': time_coord}).chunk()
    return build_rot_mat(roll_xarr, pitch_xarr, yaw_xarr, order='rpy', degrees=True)


def combine_rotation_matrix(mat_one, mat_two):
    """
    Composing two rotation matrices is performed by taking the product of the two matrices

    Assumes one of the input matrices is of size one (the mounting angle matrix, attitude changes over time)

    Order is important here.

    Parameters
    ----------
    mat_one: xarray Dataarray, 3dim rotation matrix (time, x, y)
    mat_two: xarray Dataarray, 3dim rotation matrix (time, x, y)

    Returns
    -------
    final_rot: dask Array, 3dim rotation matrix (time, x, y) for each time in input matrices

    """
    # This is apparently close, but not the right expression.  I can't figure this out right now
    # final_rot = einsum('ijk,jkl->ijk', mat_one, mat_two)

    # find the one element matrix
    if mat_one.shape[0] == 1:
        mat_one = mat_one.values
    elif mat_two.shape[0] == 1:
        mat_two = mat_two.values
    else:
        raise NotImplementedError('One of the input matrices must only have one value in the time dimension')

    # we'll just brute force it for now
    r00 = (mat_one[:, 0, 0] * mat_two[:, 0, 0]) + (mat_one[:, 1, 0] * mat_two[:, 0, 1]) + (mat_one[:, 2, 0] * mat_two[:, 0, 2])
    r00['y'] = 0
    r01 = (mat_one[:, 0, 1] * mat_two[:, 0, 0]) + (mat_one[:, 1, 1] * mat_two[:, 0, 1]) + (mat_one[:, 2, 1] * mat_two[:, 0, 2])
    r01['y'] = 1
    r02 = (mat_one[:, 0, 2] * mat_two[:, 0, 0]) + (mat_one[:, 1, 2] * mat_two[:, 0, 1]) + (mat_one[:, 2, 2] * mat_two[:, 0, 2])
    r02['y'] = 2
    r0 = xr.concat([r00, r01, r02], dim='y')
    r0['x'] = 0

    r10 = (mat_one[:, 0, 0] * mat_two[:, 1, 0]) + (mat_one[:, 1, 0] * mat_two[:, 1, 1]) + (mat_one[:, 2, 0] * mat_two[:, 1, 2])
    r10['y'] = 0
    r11 = (mat_one[:, 0, 1] * mat_two[:, 1, 0]) + (mat_one[:, 1, 1] * mat_two[:, 1, 1]) + (mat_one[:, 2, 1] * mat_two[:, 1, 2])
    r11['y'] = 1
    r12 = (mat_one[:, 0, 2] * mat_two[:, 1, 0]) + (mat_one[:, 1, 2] * mat_two[:, 1, 1]) + (mat_one[:, 2, 2] * mat_two[:, 1, 2])
    r12['y'] = 2
    r1 = xr.concat([r10, r11, r12], dim='y')
    r1['x'] = 1

    r20 = (mat_one[:, 0, 0] * mat_two[:, 2, 0]) + (mat_one[:, 1, 0] * mat_two[:, 2, 1]) + (mat_one[:, 2, 0] * mat_two[:, 2, 2])
    r20['y'] = 0
    r21 = (mat_one[:, 0, 1] * mat_two[:, 2, 0]) + (mat_one[:, 1, 1] * mat_two[:, 2, 1]) + (mat_one[:, 2, 1] * mat_two[:, 2, 2])
    r21['y'] = 1
    r22 = (mat_one[:, 0, 2] * mat_two[:, 2, 0]) + (mat_one[:, 1, 2] * mat_two[:, 2, 1]) + (mat_one[:, 2, 2] * mat_two[:, 2, 2])
    r22['y'] = 2
    r2 = xr.concat([r20, r21, r22], dim='y')
    r2['x'] = 2

    rmat = xr.concat([r0, r1, r2], dim='x').transpose('time', 'x', 'y')
    return rmat
