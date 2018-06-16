package main

import (
	"github.com/glerchundi/sqlboiler-crdb/driver"
	"github.com/volatiletech/sqlboiler/drivers"
)

func main() {
	drivers.DriverMain(&driver.CockroachDBDriver{})
}
