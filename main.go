package main

import (
	"fmt"
	"os"

	"github.com/aarondl/sqlboiler/v4/drivers"
	"github.com/glerchundi/sqlboiler-crdb/v4/driver"
)

func main() {
	if len(os.Args) == 1 {
		fmt.Fprintf(os.Stderr, "Version: v4")
		return
	}
	drivers.DriverMain(&driver.CockroachDBDriver{})
}
