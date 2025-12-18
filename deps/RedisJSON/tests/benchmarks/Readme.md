# Context

The automated benchmark definitions included within `tests/benchmarks` folder, provides a framework for evaluating and comparing feature branches and catching regressions prior to letting them into the master branch.

To be able to run local benchmarks you need `redisbench_admin>=0.1.74` [[tool repo for full details](https://github.com/RedisLabsModules/redisbench-admin)] and the benchmark tool specified on each configuration file. You can install `redisbench-admin` via PyPi as any other package.
```
pip3 install redisbench_admin>=0.1.74
```

## Usage

- Local benchmarks: `make benchmark`
- Remote benchmarks:  `make benchmark REMOTE=1`


## Included benchmarks

Each benchmark requires a benchmark definition yaml file to present on the current directory. The benchmark spec file is fully explained on the following link: https://github.com/RedisLabsModules/redisbench-admin/tree/master/docs

