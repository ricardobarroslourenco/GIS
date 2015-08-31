# Distributed processing of Landsat imagery with GDAL and Apache Spark

__author__ = "ricardo barros lourenco"
__date__ = "$12/06/2015 16:46:34$"

import osr
import os
import numpy as np
import gdal
from osgeo import gdal_array
from gdalconst import *
import struct
os.chdir("/data/")

#Open and serialize Landsat Image Band #4
filename = 'LC80230312015157LGN00'
dataset = gdal.Open((filename+'_B4.TIF'), GA_ReadOnly)
band = dataset.GetRasterBand(1)
scanline = band.ReadRaster( 0, 0, band.XSize, 1, band.XSize, 1, GDT_Float32 )
tuple_b4 = struct.unpack('f' * band.XSize, scanline)

#Open and serialize Landsat Image Band #5
dataset = gdal.Open((filename+'_B5.TIF'), GA_ReadOnly)
band = dataset.GetRasterBand(1)
projection = dataset.GetProjection()
scanline = band.ReadRaster( 0, 0, band.XSize, 1, band.XSize, 1, GDT_Float32 )
tuple_b5 = struct.unpack('f' * band.XSize, scanline)

# Initialize dictionaries (K V pairs)
kvb4 = {}
kvb5 = {}

for i in range(len(tuple_b4)):
     kvb4[i] = tuple_b4[i]

for i in range(len(tuple_b5)):  #Load array on dict
     kvb5[i] = tuple_b5[i]

b4RDD = sc.parallelize(kvb4.items()) # Creates Spark RDD Context
b5RDD = sc.parallelize(kvb5.items())
b5b4RDD = b5RDD.join(b4RDD) # Inner join by key

vectorAdd = lambda x, y: x + y  #NDVI sub calc
vectorSub = lambda x, y: x - y


normRDD = b5b4RDD.map(lambda x: np.float64(vectorAdd(x[1][0], x[1][1]))  / np.float64(vectorSub(x[1][0], x[1][1])))
norm = normRDD.collect()  #collect each KV pair and serialize output 
