package main

import (
	"fmt"
	"os"

	"github.com/infratographer/sqlboiler-crdb/v4/driver"
	"github.com/volatiletech/sqlboiler/v4/drivers"
)

func main() {
	if len(os.Args) == 1 {
		fmt.Fprintf(os.Stderr, "Version: v4")
		return
	}
	drivers.DriverMain(&driver.CockroachDBDriver{})
}
