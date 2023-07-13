# sqlboiler-crdb

## Original Source

This is a fork from https://github.com/glerchundi/sqlboiler-crdb. We, the Metal Toolbox community, plan to maintain this fork and keep it active.

## Installation

Installation is simple, just use go get. Once the binary is in your path sqlboiler will be able to use it if you run it with the driver name `crdb`.
```
# Install sqlboiler crdb driver
go get -u github.com/infratographer/sqlboiler-crdb/v4
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
* I don't plan to support other than latest version of SQLBoiler.
Although, and in order to avoid confussion, major version appears in the import path.
* Cockroach 2.x and greater are supported, no plans to add support for previous versions.
* Code generation against secure clusters is not tested yet.
