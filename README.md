# sqlboiler-crdb
[![Build Status](https://travis-ci.org/glerchundi/sqlboiler-crdb.svg?branch=master)](https://travis-ci.org/glerchundi/sqlboiler-crdb)

## Installation

Installation is simple, just use go get. Once the binary is in your path sqlboiler will be able to use it if you run it with the driver name `crdb`.
```
# Install sqlboiler crdb driver
go get -u github.com/glerchundi/sqlboiler-crdb
# Generate models
sqlboiler crdb
```
It's configuration keys in sqlboiler are simple:
```
[crdb]
user="root"
pass=""
host="localhost"
port=26257
dbname="mydatabase"
sslmode="disable"
```

**Notes**:
* Cockroach 2.x and greater are supported, no plans to add support for previous versions.
* Code generation against secure clusters is not tested yet.

## Development

This does use go-bindata (be sure to use the latest version of [kevin burke's fork](https://github.com/kevinburke/go-bindata)) to embed templates into the binary. You can run go-generate in the driver folder to re-gen the bindata after modifying templates. Other than that go build should be able to be used to build the binary.
