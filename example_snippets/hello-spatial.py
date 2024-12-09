import sys

try:
    from osgeo import ogr, osr, gdal
    print('jupp')
except:
    print('ERROR: cannot find GDAL/OGR modules')