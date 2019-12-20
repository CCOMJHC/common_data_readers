# par3 module - NOAA
par3 module (originally developed by Glen) used by NOAA for reading .all files

Can be used in one of three major ways:

## 1. Mapping/Seeking records:

> import par3 as par

> ar = par.AllRead(r'C:\Collab\dasktest\0009_20170523_181119_FA2806.all')

> rec = ar.getrecord(78,0)

> rec.display()

> Out:

> Counter : 61966

> Serial# : 40111

> SoundSpeed : 1488.6

> Ntx : 3

> Nrx : 400

> Nvalid : 373

> SampleRate : 9077.705

> Dscale : 65534

> rec.rx_data.BeamPointingAngle

> Out: 

>array([ 74.58      ,  74.47      ,  74.36      ,  74.25      ,
        74.13      ,  74.02      ,  73.85      ,  73.74      ,
        73.619995  ,  73.5       ,  73.38      ,  73.25      ,
        73.07      ,  72.95      ,  72.82      ,  72.689995  ,
        72.57      ,  72.439995  ,  72.159996  ,  72.03      ,
        71.9       ,  71.76      ,  71.619995  ,  71.49      ,
        71.35      ,  71.2       ,  71.06      ,  70.92      ,
        70.77      ,  70.619995  ,  70.4       ,  70.25      ,
        70.1       ,  69.939995  ,  69.78      ,  69.63      ,
        69.47      ,  69.229996  ,  69.07      ,  68.9       ,
        68.729996  ,  68.56      ,  68.229996  ,  68.06      ,
        67.88      ,  67.7       ,  67.52      ,  67.34      ,
        67.15      ,  66.97      ,  66.78      ,  66.59      ,
        66.39      ,  66.189995  ,  65.9       ,  65.7       ,
        65.5       ,  65.29      ,  65.08      ,  64.869995  ,
        64.65      ,  64.26      ,  64.04      ,  63.809998  ,
        63.59      ,  63.359997  ,  63.12      ,  62.89      ,
        62.649998  ,  62.41      ,  62.16      ,  61.91      ,
        61.66      ,  61.399998  ,  60.859997  ,  60.59      ,
        60.329998  ,  60.059998  ,  59.78      ,  59.51      ,
        59.219997  ,  58.94      ,  58.649998  ,  58.359997  ,
        58.059998  ,  57.76      ,  57.449997  ,  57.14      ,
        56.829998  ,  56.51      ,  56.18      ,  55.85      ,
        55.52      ,  55.18      ,  54.84      ,  54.489998  ,
        54.039997  ,  53.68      ,  53.309998  ,  52.94      ,
        52.57      ,  52.19      ,  51.8       ,  51.309998  ,
        50.91      ,  50.51      ,  50.1       ,  49.69      ,
        49.26      ,  48.84      ,  48.399998  ,  47.62      ,
        47.17      ,  46.719997  ,  46.26      ,  45.789997  ,
        45.32      ,  44.829998  ,  44.35      ,  43.85      ,
        43.35      ,  42.84      ,  42.32      ,  41.79      ,
        41.26      ,  40.719997  ,  40.17      ,  39.62      ,
        39.05      ,  38.48      ,  37.899998  ,  37.309998  ,
        36.719997  ,  36.11      ,  35.5       ,  34.88      ,
        34.25      ,  33.62      ,  32.969997  ,  32.32      ,
        31.55      ,  30.88      ,  30.199999  ,  29.519999  ,
        28.83      ,  28.13      ,  27.42      ,  26.699999  ,
        25.98      ,  25.25      ,  24.51      ,  23.76      ,
        23.01      ,  22.25      ,  21.48      ,  20.59      ,
        19.81      ,  19.029999  ,  18.24      ,  17.44      ,
        16.64      ,  15.83      ,  15.0199995 ,  14.2       ,
        13.38      ,  12.55      ,  11.719999  ,  10.889999  ,
        10.059999  ,   9.22      ,   8.38      ,   7.54      ,
         6.69      ,   5.85      ,   5.        ,   4.15      ,
         3.31      ,   2.46      ,   1.61      ,   0.77      ,
        -0.08      ,  -0.91999996,  -1.76      ,  -2.6       ,
        -3.4299998 ,  -4.2599998 ,  -5.0899997 ,  -5.92      ,
        -6.74      ,  -7.5499997 ,  -8.36      ,  -9.17      ,
        -9.969999  , -10.7699995 , -11.559999  , -12.219999  ,
       -13.        , -13.7699995 , -14.54      , -15.299999  ,
       -16.05      , -16.789999  , -17.529999  , -18.26      ,
       -18.98      , -19.699999  , -20.41      , -21.109999  ,
       -21.8       , -22.48      , -23.05      , -23.71      ,
       -24.369999  , -25.019999  , -25.67      , -26.3       ,
       -26.93      , -27.55      , -28.16      , -28.76      ,
       -29.359999  , -29.939999  , -30.519999  , -31.09      ,
       -31.66      , -32.21      , -32.76      , -33.3       ,
       -33.829998  , -34.35      , -34.87      , -35.38      ,
       -35.88      , -36.38      , -36.86      , -37.34      ,
       -37.82      , -38.28      , -38.739998  , -38.86      ,
       -39.3       , -39.739998  , -40.18      , -40.6       ,
       -41.02      , -41.44      , -41.85      , -42.25      ,
       -42.55      , -42.94      , -43.32      , -43.7       ,
       -44.079998  , -44.45      , -44.809998  , -45.17      ,
       -45.42      , -45.77      , -46.11      , -46.45      ,
       -46.78      , -47.11      , -47.43      , -47.75      ,
       -48.07      , -48.379997  , -48.68      , -48.98      ,
       -49.28      , -49.57      , -49.86      , -50.14      ,
       -50.43      , -50.7       , -50.98      , -51.239998  ,
       -51.51      , -51.77      , -52.03      , -52.        ,
       -52.25      , -52.5       , -52.739998  , -52.98      ,
       -53.219997  , -53.46      , -53.69      , -53.92      ,
       -54.14      , -54.37      , -54.59      , -54.809998  ,
       -55.02      , -55.23      , -55.44      , -55.649998  ,
       -55.67      , -55.87      , -56.07      , -56.27      ,
       -56.46      , -56.649998  , -56.84      , -57.03      ,
       -57.219997  , -57.309998  , -57.489998  , -57.67      ,
       -57.84      , -58.02      , -58.19      , -58.359997  ,
       -58.53      , -58.69      , -58.859997  , -59.02      ,
       -59.18      , -59.34      , -59.489998  , -59.649998  ,
       -59.8       , -59.949997  , -60.1       , -60.25      ,
       -60.239998  , -60.379997  , -60.52      , -60.67      ,
       -60.809998  , -60.94      , -61.079998  , -61.219997  ,
       -61.35      , -61.48      , -61.609997  , -61.739998  ,
       -61.8       , -61.93      , -62.05      , -62.18      ,
       -62.3       , -62.42      , -62.539997  , -62.66      ,
       -62.78      , -62.899998  , -63.01      , -63.129997  ,
       -63.17      , -63.28      , -63.399998  , -63.51      ,
       -63.609997  , -63.719997  , -63.829998  , -63.94      ,
       -64.04      , -64.14      , -64.25      , -64.35      ,
       -64.45      , -64.549995  , -64.65      , -64.75      ,
       -64.84      , -64.939995  , -65.03      , -65.13      ,
       -65.22      , -65.31      , -65.409996  , -65.5       ,
       -65.59      , -65.68      , -65.77      , -65.71      ,
       -65.799995  , -65.89      , -65.97      , -66.06      ,
       -66.14      , -66.22      , -66.31      , -66.39      ,
       -66.47      , -66.549995  , -66.63      , -66.71      ,
       -66.79      , -66.86      , -66.88      , -66.96      ,
       -67.03      , -67.11      , -67.18      , -67.25      ,
       -67.33      , -67.4       , -67.47      , -67.54      ],
      dtype=float32)

## 2. Read desired record(s) from start point to end point:

> import par3 as par

> fil = r'C:\collab\dasktest\0009_20170523_181119_FA2806.all'

> chnkedfile = par.return_chunked_fil(fil, 0, par.determine_good_chunksize(fil, 1000000, 20))

> C:\Collab\dasktest\0009_20170523_181119_FA2806.all: Using 7 chunks of size 1121689

> chnkedfile

> Out: 

> [['C:\\Collab\\dasktest\\0009_20170523_181119_FA2806.all', 0, 1121689],

> ['C:\\Collab\\dasktest\\0009_20170523_181119_FA2806.all', 1121689, 2243378],

> ['C:\\Collab\\dasktest\\0009_20170523_181119_FA2806.all', 2243378, 3365067],

> ['C:\\Collab\\dasktest\\0009_20170523_181119_FA2806.all', 3365067, 4486756],

> ['C:\\Collab\\dasktest\\0009_20170523_181119_FA2806.all', 4486756, 5608445],

> ['C:\\Collab\\dasktest\\0009_20170523_181119_FA2806.all', 5608445, 6730134],

> ['C:\\Collab\\dasktest\\0009_20170523_181119_FA2806.all', 6730134, 7851828]]

> parchunks = []

> for chnk in chnkedfile:

> parchunks.append(par.AllRead(chnk[0], start_ptr=chnk[1], end_ptr=chnk[2]).sequential_read_records())
        
> parchunks[0]['78']['beampointingangle']

> Out: 

> array([[ 74.58000183,  74.47000122,  74.36000061, ...,   0.        ,
           0.        ,   0.        ],
        [ 43.84999847,  43.34999847,  42.84000015, ...,   0.        ,
           0.        ,   0.        ],
        [-36.38000107, -36.86000061, -37.34000015, ...,   0.        ,
           0.        ,   0.        ],
        ...,
        [ 70.11000061,  70.        ,  69.76000214, ...,   0.        ,
           0.        ,   0.        ],
        [ 39.18999863,  38.68999863,  38.18999863, ...,   0.        ,
          0.        ,   0.        ],
        [-40.96999741, -41.45999908, -41.79000092, ...,   0.        ,
          0.        ,   0.        ]])
          
## 3. BatchRead class and Xarray
 
 Requires Xarray / Dask to function!  Reads all .all files in a given folder, returns xarray Dataset
 
 > import par3 as par
 
 > b_read = par.BatchRead(r'C:\collab\dasktest')
 
 > b_read.read()
 
 > Out:
 
 > Running Kongsberg .all converter...

 > C:\collab\dasktest\0009_20170523_181119_FA2806.all: Using 4 chunks of size 1962957

 > Distributed conversion complete: 2.5396290000000006s
 
 > distributed.nanny - WARNING - Worker process still alive after 4 seconds, killing
 
 > distributed.nanny - WARNING - Worker process still alive after 4 seconds, killing

 > distributed.nanny - WARNING - Worker process still alive after 4 seconds, killing

 > distributed.nanny - WARNING - Worker process still alive after 4 seconds, killing

 > Found 1 total Installation Parameters entr(y)s
 
 > Constructed offsets successfully
 
 > b_read.rawdat
 
> Out: 

> <xarray.Dataset>

> Dimensions:             (beam: 182, sectors: 3, time: 216)

> Coordinates:

>   * beam                (beam) int64 0 1 2 3 4 5 6 ... 176 177 178 179 180 181

>   * sectors             (sectors) int32 0 1 2

>   * time                (time) float64 1.496e+09 1.496e+09 ... 1.496e+09

> Data variables:

>     roll                (time) float32 dask.array<chunksize=(55,), meta=np.ndarray>

>     pitch               (time) float32 dask.array<chunksize=(55,), meta=np.ndarray>

>     heave               (time) float32 dask.array<chunksize=(55,), meta=np.ndarray>

>     heading             (time) float32 dask.array<chunksize=(55,), meta=np.ndarray>

>     soundspeed          (time, sectors) float32 dask.array<chunksize=(55, 3), meta=np.ndarray>

>     ntx                 (time, sectors) uint16 dask.array<chunksize=(55, 3), meta=np.ndarray>

>     nrx                 (time, sectors) uint16 dask.array<chunksize=(55, 3), meta=np.ndarray>

>     nvalid              (time, sectors) uint16 dask.array<chunksize=(55, 3), meta=np.ndarray>

>     samplerate          (time, sectors) float32 dask.array<chunksize=(55, 3), meta=np.ndarray>

>     transmitsector#     (time, sectors) uint8 dask.array<chunksize=(55, 3), meta=np.ndarray>

>     tiltangle           (time, sectors) float32 dask.array<chunksize=(55, 3), meta=np.ndarray>

>     signallength        (time, sectors) float32 dask.array<chunksize=(55, 3), meta=np.ndarray>

>     delay               (time, sectors) float32 dask.array<chunksize=(55, 3), meta=np.ndarray>

>     frequency           (time, sectors) float32 dask.array<chunksize=(55, 3), meta=np.ndarray>

>     waveformid          (time, sectors) uint8 dask.array<chunksize=(55, 3), meta=np.ndarray>

>     beampointingangle   (time, sectors, beam) float64 dask.array<chunksize=(55, 3, 182), meta=np.ndarray>

>     transmitsectorid    (time, sectors, beam) float64 dask.array<chunksize=(55, 3, 182), meta=np.ndarray>

>     detectioninfo       (time, sectors, beam) float64 dask.array<chunksize=(55, 3, 182), meta=np.ndarray>

>     qualityfactor       (time, sectors, beam) float64 dask.array<chunksize=(55, 3, 182), meta=np.ndarray>

>     traveltime          (time, sectors, beam) float64 dask.array<chunksize=(55, 3, 182), meta=np.ndarray>

>     latitude            (time) float64 dask.array<chunksize=(55,), meta=np.ndarray>

>     longitude           (time) float64 dask.array<chunksize=(55,), meta=np.ndarray>

>     alongtrackvelocity  (time) float32 dask.array<chunksize=(55,), meta=np.ndarray>

>     altitude            (time) float64 dask.array<chunksize=(55,), meta=np.ndarray>

> Attributes:

>     settings_1495563079:  {"waterline_vertical_location": "-0.640", "system_m...

>     profile_1495599960:   [[0.0, 1489.2000732421875], [0.32, 1489.20007324218...
