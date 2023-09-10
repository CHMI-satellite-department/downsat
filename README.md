# Downsat

Downsat offers a simplified approach to downloading and managing meteorological satellite data, along with its derived products.

**Compatibility:** `Python >= 3.9`.

**Status:** 🚧 Early Development Stage

**Note:** This project is in an early stage of development. While we are committed to maintaining a stability of the library, please be aware that bugs and future breaking changes are possible. We will make an effort to minimize the impact on users, but some caution is currently advised when using it.

## Features

- unified dict-like access to local and remote satellite data and products
- automatic caching of downloaded data and created products

## Supported satellites and data types

### [Eumetsat](#downloading-data)

- MSG: `from downsat import MSG`
- RSS: `from downsat import RSS`
- Metop: `from downsat import Metop`

### [Satpy](#satpy-1)

- Satpy scene: `from downsat import SatpyScene`
- Satpy composite image: `from downsat import SatpyProduct`

### [Historical TLE - spacetrack](#tle)

- grouped by day: `from downsat import DailyTLE`

### Extra functionality

- [find polar satellites that observed given point around given time](#dowloading-data-from-leo-satellites-that-saw-a-particular-point-on-earth-around-given-time)

## Installation

```shell
pip install 'downsat[pytroll] @ git+ssh://git@github.com/CHMI-satellite-department/downsat.git'
```

or

```shell
pip install 'downsat[pytroll] @ git+https://github.com/CHMI-satellite-department/downsat.git'
```

Note that in this example we have installed also the `[pytroll]` extras. It is not obligatory but without that, `downsat` will not:

-  accept specification of the region of interest by satpy-defined areas such as `eurol`
-  be able to determine polar satellites that saw given point at given time

If you do not need these functionalities, the installation command simplifies to

```shell
pip install git+https://github.com/CHMI-satellite-department/downsat.git
```

**Note:** Installation using PyPI Python Package Index is currently a work in progress.

## Usage

### Downloading data

```python
from downsat import MSG

# specify where to store data
msg_archive_path = '/path/to/archive/MSG'

# Create an object representing MSG archive
msg = MSG.from_env(data_path=msg_archive_path)

# Download one time slot
filenames = msg['202211041230']  # download will take few minutes
# [2]: (Path(filename1), )

# Get the same data from the cache
filenames = msg['202211041230']  # will take few seconds
# [2]: (Path(filename1), )
```

In the example above, Eumetsat Data Store credentials are automatically loaded from `EUMDAC_KEY` and `EUMDAC_SECRET` environment variables. Alternatively, they can be specified explicitly:

```python
from downsat import EumdacKey

key = EumdacKey(key="my-key", secret="my-secret")
msg = MSG(credentials=key, data_path=msg_archive_path)

# or
msg = MSG.from_env(credentials=key, data_path=msg_archive_path)
```

Download paralellization can be specified as well. At this moment it is, however, not recommended to use more than 3 threads due to eumdac user quotas.
```python
msg = MSG.from_env(data_path='/path/to/archive/MSG', num_workers=3)
```




### Bulk download

The data accessors accept either precies `datetime.datetime` object or a time range. This can be done either using slice `slice(datetime.datetime(2020, 12, 1), datetime.datetime(2020, 12, 2))` or by a string such as `202012` (one month), `20201201` (one day),  `2020120114` (one hour), `2020120105` (one minute).

Several other string formats such as `2020-11-04 15h` are supported.

Time range can be specified also by slice of two strings `slice("20201201", "20201202")`.

The accessors can even download multiple times or time ranges at once:

```python
filenames = msg["2022-11-04 13:30", "2022-11-05 12h"]
```

and the result will be single tuple containing all files containg data from the specified times.

**Note:** When a single datetime object is given, the seconds are stripped down and it represents a whole minute range.

### Narrowing down the search

Eumdac client can accept additional query parameters such as region of interest or satellite specification. These can be specified during construction of the `MSG` and `Metop` objects and will be applied in all data requests.

```python
msg = MSG.from_env(data_path='/path/to/archive/MSG4', sat="MSG4")
```

To see all options available for a given satellite, run

```python
from downsat import EumdacCollection

EumdacCollection(name='MSG', credentials=key).collection.search_options
```

Note: This will be simplified in the future.

**Warning:** There is currently [a bug in the eumdac client](https://eumetsatspace.atlassian.net/wiki/spaces/EUM/blog/2022/10/24/1908965377/Data+Store+APIs+1.0.0+released#Known-issues) and if region specification is used when requesting MSG data, eumdac client will not return any data.

### Downloading data that cover certain point or region of interest

To simplify selection of the region of interest, downsat adds two special query parameters:

- `area` returns only data that overlap with a given `pyresample.AreaDefinition` or satpy area specified by a string (when downsat was installed with `satpy` extras such as `pip install downsat[pytroll]`).

```python
from downsat import Metop

metop = Metop.from_env(data_path=archive_path, area='eurol')
metop['2022110411']
```

- `point` returns only data that contain given longitude and latitude

```python
from downsat import LonLat

metop = Metop.from_env(data_path=archive_path, point=LonLat(lon=14.46, lat=50.))
metop['2022110411']
```

**Warning:** There is currently [a bug in the eumdac client](https://eumetsatspace.atlassian.net/wiki/spaces/EUM/blog/2022/10/24/1908965377/Data+Store+APIs+1.0.0+released#Known-issues) and if region specification is used when requesting MSG data, eumdac client will not return any data.

### Satpy

The downsat data sources are well-suited for integration with satpy

#### Creating satpy product

```python
from downsat import SatpyProduct

msg = MSG.from_env(data_path=data_path)
natural_color = SatpyProduct(msg, "natural_color", area="eurol")

# get the product as trollimage.xrimage.XRImage
image = natural_color["2022-11-04 12:30"]

# get multiple products at once
image1, image2 = natural_color["2022-11-04 12:30", "2022-11-04 12:45"]
```

#### Creating satpy Scene automatically

```python
from downsat import MSG, SatpyScene

msg = MSG.from_env(data_path=data_path, flatten=False)
scenes = SatpyScene(msg, reader='seviri_l1b_native', channels='IR_108', area="eurol", flatten=False)

scns = scenes['2022-11-04 12:30', '2022-11-04 12:45']
assert len(scns) == 2
```

**Gotcha:** Trying to load multiple scenes at once like `scn['2022-11-04 12:30', '2022-11-04 13:30']` without setting `flatten` to false in both the source and `SatpyScene` would return a single `Scene` with data loaded from both/all time slots. To get a separate scene for each key, both `MSG` and `SatpyScene` classes must get argument `flatten=False`.


#### Creating satpy Scene manually

```python
from satpy import Scene

scene = Scene(filenames=msg['2022-11-04 12:30'], reader='seviri_l1b_native')
```

Of course, other datetime formats such as `'202211041230'`  can be used as well.

### TLE

Downsat offers easy access to historical daily TLE data for any object in the SpaceTrack database. Downloaded data are cached localy in a file archive so that repeated query is faster and saves your SpaceTrack usage quotas.

```python
from downsat import DailyTLE, SpaceTrackKey

# Load spacetrack key from SPACETRACK_USERNAME and SPACETRACK_PASSWORD env variables.
# Alternatively, the credentials mey be specified directly: `SpaceTrackKey(username=..., password=...)`.
key = SpaceTrackKey.from_env()

# Specify location of the data archive and object whose data to download.
# The object can be specified either by its SpaceTrack name (str) or norad_cat_id (int).
tle_archive = DailyTLE(object_id='METOP-B', credentials=key, data_path='/my/tle/archive')

# download data for a particular date (can be string or datetime.date or datetime.datetime object)
tle = tle_archive['2023-06-21']
# (Path('/my/tle/archive/METOP-B/2023-06-21'),)
```

### Satellite parameters

For its internal purposes, downsat maintains a limited datbase of parmeters of satellites and their instruments.

```python
from downsat import satellite_info

metop_a = satellite_info["METOP-A"]
metop_a, metop_b = satellite_info["METOP-A", "METOP-B"]
```

`satellite_info` provides information on both polar and geostationary satellite. To limit the selection to only
one of those, import `satellite_info_leo` or `satellite_info_geo` instead.

### Dowloading data from LEO satellites that saw a particular point on Earth around given time.

Sometimes you need to compare specific objects on geostationary imagery with higher-resolution data from polar satellites. As a first
step it is necessary to find if any data from polar satellites are available. You can query a specific satellite,

```python
from functools import partial
import datetime

from downsat import DailyTLE, LonLat, SpaceTrackKey
from downsat.query.polar import find_visible_polar_passes

key = SpaceTrackKey.from_env()  # or SpaceTrackKey(username=..., password=...)

prague = LonLat(lon=14.41854, lat=50.07366)
time = "2023-06-20 9:00"
dt = datetime.timedelta(hours=1)  # time tolerance +- 1 hour
tle_archive = partial(DailyTLE, credentials=key, data_path="/path/to/tle/archive")

metop_a_passes = find_visible_polar_passes(prague, time, tle_archive, satellite='METOP-A', dt=dt)
# (<Arrow [2023-06-20T0...07+00:00]>,)
```

The returned time is a time when the satellite was closest to the point of interest.

**Note:** Using `partial` to construct `tle_archive` won't be necessary in future releases. Removing it is currently a work in progress.

By default, `find_visible_polar_passes` uses swath angle of the satellite instrument defined in `downsat.config.satellites`. If you
want to change this parameter or query a satellite that is not stored in this internal database, you can specify this parameter
explicitly. The angle is given from the observers point, i.e. `90 - satellite_swath_angle`

```python
custom_swath_angle = 40  # [deg]
metop_a_passes = find_visible_polar_passes(prague, time, tle_archive, satellite='METOP-A', dt=dt, horizon=90 - custom_swath_angle)
# ()
```

The function also accepts an iterable of satellite names. In such case it returns a dictionary `{satellite_name: pass_times}`.
If satellite name is not given at all, `find_visible_polar_passes` finds passes of all known polar satellites, i.e. polar
satellites stored in the `satellite_info_leo` data source.

```
all_passes = find_visible_polar_passes(prague, time, tle_archive, dt=dt)
```