# This script converts HDF5 int8 data to uint8 and saves it to a new file.

import h5py  # Library to handle HDF5 files
import numpy as np  # Library for numerical operations
import os  # Library for operating system dependent functionality

# Directory where the output file will be saved
OUTPUT_DIR = 'uint8/'
# Create the output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Name of the input HDF5 file
hdf5_file = 'wikipedia-1024_eng_v3_single.hdf5'

# Open the input HDF5 file in read mode
with h5py.File(hdf5_file, "r") as f:
    # Load the 'train' dataset into memory as a numpy array
    dataset = np.array(f['train'])
    # Load the 'test' dataset into memory as a numpy array
    test = np.array(f['test'])

# Generate the name for the output HDF5 file
output_file = hdf5_file.replace('.hdf5', '_uint8.hdf5')
# Join the output directory path with the output file name
output_file = os.path.join(OUTPUT_DIR, output_file)

# Convert the 'test' dataset from int8 to uint8
# First, cast to int16 and add 128 to shift the range from [-128, 127] to [0, 255]
# Then, cast to uint8
test = np.astype(test.astype(np.int16) + 128, np.uint8)
# Convert the 'train' dataset from int8 to uint8 using the same method
dataset = np.astype(dataset.astype(np.int16) + 128, np.uint8)

# Open the output HDF5 file in write mode
with h5py.File(output_file, "w") as f:
    # Create a new dataset 'train' in the output file with the converted data
    f.create_dataset('train', data=dataset)
    # Create a new dataset 'test' in the output file with the converted data
    f.create_dataset('test', data=test)
