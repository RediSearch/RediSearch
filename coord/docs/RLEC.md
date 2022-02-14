## Installing RSCoordinator on Redis Enterprise Cluster

There is an automated [fabric](http://fabfile.org) based script to install and configure a search cluster using RSCoordinator and RediSearch, under the `fab` folder in this repo. To install and configure a cluster, follow the following steps:

### 0. Install and configure RLEC cluster

The steps for that are not detailed in this scope. We assume you have a cluster ready with a version that supports modules. 

### 1. Install fabric

(Assuming you have pip installed, if not install it first):

```sh
$ sudo pip install fabric
```

### 2. Configure S3 credentials

You must provide an s3 configuration file that has access to the `s3://redismodules` bucket, where the module builds are stored.
Copy that file as `s3cfg` to `fab/res` inside the RSCoordinator project:

```sh
$ cp /path/to/.s3cfg /path/to/RSCoordinator/fab/res/s3cfg
```

### 3. Download the modules to the cluster master machine

```sh
$ cd RSCoordinator/fab
# Change the ip of the RLEC cluster master accordingly
$ export RL_MASTER=1.2.3.4
$ fab download_modules
```

### 4. Create the database with the modules

You need to provide:
1. The desired database name
2. The number of shards

```sh
$ cd RSCoordinator/fab
# Change the ip of the RLEC cluster master accordingly
$ export RL_MASTER=1.2.3.4
# Change the database name and number of shards accordingly
$ fab create_database:my_db,10
```
