import mpi4py.MPI as MPI
import numpy as np
import binascii
import json


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

file_name = "tinyTwitter.json"
amode = MPI.MODE_RDONLY
mpi_file = MPI.File.Open(comm, file_name, amode)
buffer = np.empty(100)
buffer[:] = rank
mpi_file.Read(buffer)
mpi_file.Close()

print(buffer)
for b in buffer:
    b = int(b)
    x = b.to_bytes((b.bit_length() + 7) // 8, 'big').decode()

    print(x)
#t=json.load()

mpi_file.Close()
#print(mpi_file)
#
# data = bytearray()
# buf=[data, 100]
#
# s = mpi_file.Read(buf)
# print(s)
    #     self.data_dim = np.zeros(1, dtype=np.dtype('i4'))
    #     self.n_particles = np.zeros(1, dtype=np.dtype('i4'))
    #     self.file_name = file_name
    #     self.debug = True
    #
    # def info(self):
    #     """ Distrubute the required information for reading to all ranks.
    #
    #     Every rank must run this funciton.
    #     Each machine needs data_dim and n_particles.
    #     """
    #     # get info on all machines
    #     self.mpi_file.Read_all([self.data_dim, MPI.INT])
    #     self.mpi_file.Read_all([self.n_particles, MPI.INT])
    #     self.data_start = self.mpi_file.Get_position()
    #
    # def read(self):
    #     """ Read data and return the processors part of the coordinates to:
    #         self.x_proc
    #         self.y_proc
    #         self.z_proc
    #     """
    #     assert self.data_dim != 0
    #     # First establish rank's vector sizes
    #     default_size = np.ceil(self.n_particles / self.size)
    #     # Rounding errors here should not be a problem unless
    #     # default size is very small
    #     end_size = self.n_particles - (default_size * (self.size - 1))
    #     assert end_size >= 1
    #     if (self.rank == (self.size - 1)):
    #         self.proc_vector_size = end_size
    #     else:
    #         self.proc_vector_size = default_size
    #     # Create individual processor pointers
    #     #
    #     x_start = int(self.data_start + self.rank * default_size *
    #             self.data_type_size)
    #     y_start = int(self.data_start + self.rank * default_size *
    #             self.data_type_size +  self.n_particles *
    #             self.data_type_size * 1)
    #     z_start = int(self.data_start + self.rank * default_size *
    #             self.data_type_size + self.n_particles *
    #             self.data_type_size * 2)
    #     self.x_proc = np.zeros(self.proc_vector_size)
    #     self.y_proc = np.zeros(self.proc_vector_size)
    #     self.z_proc = np.zeros(self.proc_vector_size)
    #     # Seek to x
    #     self.mpi_file.Seek(x_start)
    #     if self.debug:
    #         print ('MPI Read')
    #     self.mpi_file.Read([self.x_proc, MPI.DOUBLE])
    #     if self.rank:
    #         print('MPI Read done')
    #     self.mpi_file.Seek(y_start)
    #     self.mpi_file.Read([self.y_proc, MPI.DOUBLE])
    #     self.mpi_file.Seek(z_start)
    #     self.mpi_file.Read([self.z_proc, MPI.DOUBLE])
    #     self.comm.Barrier()
    #     return self.x_proc, self.y_proc, self.z_proc
    #
    # def Close(self):
    #     self.mpi_file.Close()