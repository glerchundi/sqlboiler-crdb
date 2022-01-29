// These tests assume there is a user sqlboiler_driver_user and a database
// by the name of sqlboiler_driver_test that it has full R/W rights to.
// In order to create this you can use the following steps from a root
// crdb account:
//
//   create role sqlboiler_driver_user login nocreatedb nocreaterole nocreateuser password 'sqlboiler';
//   create database sqlboiler_driver_test owner = sqlboiler_driver_user;

package driver

import (
	"bytes"
	"encoding/json"
	"flag"
	"io/ioutil"
	"os/exec"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/volatiletech/sqlboiler/v4/drivers"
)

var (
	flagOverwriteGolden = flag.Bool("overwrite-golden", false, "Overwrite the golden file with the current execution results")

	envHostname = drivers.DefaultEnv("DRIVER_HOSTNAME", "localhost")
	envPort     = drivers.DefaultEnv("DRIVER_PORT", "26257")
	envUsername = drivers.DefaultEnv("DRIVER_USER", "root")
	envPassword = drivers.DefaultEnv("DRIVER_PASS", "")
	envDatabase = drivers.DefaultEnv("DRIVER_DB", "sqlboiler_driver_test")
)

func TestAssemble(t *testing.T) {
	b, err := ioutil.ReadFile("testdatabase.sql")
	if err != nil {
		t.Fatal(err)
	}

	out := &bytes.Buffer{}
	port, _ := strconv.Atoi(envPort)
	url := buildQueryString(envUsername, envPassword, envDatabase, envHostname, port, "disable")

	initDB := exec.Command("cockroach", "sql", "--url", url, "-e", "CREATE DATABASE IF NOT EXISTS "+envDatabase)
	initDB.Stdout = out
	initDB.Stderr = out
	if err := initDB.Run(); err != nil {
		t.Logf("cockroach output:\n%s\n", out.Bytes())
		t.Fatal(err)
	}
	t.Logf("cockroach output:\n%s\n", out.Bytes())
	out.Reset()

	createDB := exec.Command("cockroach", "sql", "--url", url)
	createDB.Stdout = out
	createDB.Stderr = out
	createDB.Stdin = bytes.NewReader(b)

	if err := createDB.Run(); err != nil {
		t.Logf("cockroach output:\n%s\n", out.Bytes())
		t.Fatal(err)
	}
	t.Logf("cockroach output:\n%s\n", out.Bytes())

	tests := []struct {
		name       string
		config     drivers.Config
		goldenJson string
	}{
		{
			name: "default",
			config: drivers.Config{
				"user":    envUsername,
				"pass":    envPassword,
				"dbname":  envDatabase,
				"host":    envHostname,
				"port":    envPort,
				"sslmode": "disable",
				"schema":  "public",
			},
			goldenJson: "crdb.golden.json",
		},
		{
			name: "enum_types",
			config: drivers.Config{
				"user":           envUsername,
				"pass":           envPassword,
				"dbname":         envDatabase,
				"host":           envHostname,
				"port":           envPort,
				"sslmode":        "disable",
				"schema":         "public",
				"add-enum-types": true,
			},
			goldenJson: "crdb.golden.enums.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &CockroachDBDriver{}
			info, err := c.Assemble(tt.config)
			if err != nil {
				t.Fatal(err)
			}

			got, err := json.MarshalIndent(info, "", "  ")
			if err != nil {
				t.Fatal(err)
			}

			if *flagOverwriteGolden {
				if err = ioutil.WriteFile(tt.goldenJson, got, 0664); err != nil {
					t.Fatal(err)
				}
				t.Log("wrote:", string(got))
				return
			}

			want, err := ioutil.ReadFile(tt.goldenJson)
			if err != nil {
				t.Fatal(err)
			}

			require.JSONEq(t, string(want), string(got))
		})
	}
}
