var rgxCDBFkey = regexp.MustCompile(`(?m)((\n)?.*CONSTRAINT.*?FOREIGN KEY.*?\n|(\n)?[a-zA-Z _]*VALIDATE CONSTRAINT.*?.*?\n)`)

type crdbTester struct {
  dbConn *sql.DB

  dbName      string
  host        string
  user        string
  pass        string
  sslmode     string
  sslcert     string
  sslkey      string
  sslrootcert string
  port        int

  testDBName  string

  testDBURL   string
  dbURL       string
}

func (c *crdbTester) insecureFlag() string {
	var insecure string
	if c.sslmode == "disable" {
		insecure = "--insecure"
	} else {
		insecure = ""
	}
	return insecure
}

func init() {
  dbMain = &crdbTester{}
}

// setup dumps the database schema and imports it into a temporary randomly
// generated test database so that tests can be run against it using the
// generated sqlboiler ORM package.
func (c *crdbTester) setup() error {
  var err error

  c.dbName = viper.GetString("crdb.dbname")
  c.host = viper.GetString("crdb.host")
  c.user = viper.GetString("crdb.user")
  c.pass = viper.GetString("crdb.pass")
  c.port = viper.GetInt("crdb.port")
  c.sslmode = viper.GetString("crdb.sslmode")
  c.sslcert = viper.GetString("crdb.sslcert")
	c.sslkey = viper.GetString("crdb.sslkey")
	c.sslrootcert = viper.GetString("crdb.sslrootcert")
  // Create a randomized db name.
  c.testDBName = randomize.StableDBName(c.dbName)

 	c.testDBURL = buildQueryString(c.user, c.pass, c.testDBName, c.host, c.port, c.sslmode, c.sslkey, c.sslcert, c.sslrootcert)
	c.dbURL = buildQueryString(c.user, c.pass, c.dbName, c.host, c.port, c.sslmode, c.sslkey, c.sslcert, c.sslrootcert)

  if err = c.dropTestDB(); err != nil {
    return err
  }
  if err = c.createTestDB(); err != nil {
    return err
  }

	insecure := c.insecureFlag()
  var dumpCmd, createCmd *exec.Cmd
  if len(insecure) > 0{
    dumpCmd = exec.Command("cockroach", "dump", c.dbName, "--url", c.dbURL, insecure, "--dump-mode=schema")
    createCmd = exec.Command("cockroach", "sql", "--url", c.testDBURL, insecure, "--database", c.testDBName)
  } else {
    dumpCmd = exec.Command("cockroach", "dump", c.dbName, "--url", c.dbURL, "--dump-mode=schema")
    createCmd = exec.Command("cockroach", "sql", "--url", c.testDBURL, "--database", c.testDBName)
  }

  r, w := io.Pipe()
  dumpCmd.Stdout = w
  createCmd.Stdin = newFKeyDestroyer(rgxCDBFkey, r)

  if err = dumpCmd.Start(); err != nil {
      return errors.Wrap(err, "failed to start 'cockroach dump' command")
  }
  if err = createCmd.Start(); err != nil {
      return errors.Wrap(err, "failed to start 'cockroach sql' command")
  }

  if err = dumpCmd.Wait(); err != nil {
      return errors.Wrap(err, "failed to wait for 'cockroach sql' command")
  }

  // After dumpCmd is done, close the write end of the pipe
  if err = w.Close(); err != nil {
      return errors.Wrap(err, "failed to close pipe")
  }

  if err = createCmd.Wait(); err != nil {
      return errors.Wrap(err, "failed to wait for 'cockroach sql' command")
  }

  return nil
}

func (c *crdbTester) runCmd(stdin, command string, args ...string) error {
  cmd := exec.Command(command, args...)
  cmd.Env = os.Environ()

  if len(stdin) != 0 {
    cmd.Stdin = strings.NewReader(stdin)
  }

  stdout := &bytes.Buffer{}
  stderr := &bytes.Buffer{}
  cmd.Stdout = stdout
  cmd.Stderr = stderr
  if err := cmd.Run(); err != nil {
    fmt.Println("failed running:", command, args)
    fmt.Println(stdout.String())
    fmt.Println(stderr.String())
    return err
  }

  return nil
}

func (c *crdbTester) createTestDB() error {
	insecure := c.insecureFlag()
	stmt := fmt.Sprintf("CREATE DATABASE %s", c.testDBName)
	var args []string
	if len(insecure) > 0 {
		args = []string{"sql", "--url", c.testDBURL, insecure, "--execute", stmt}
	} else {
		args = []string{"sql", "--url", c.testDBURL, "--execute", stmt}
	}
	return c.runCmd("", "cockroach", args...)
}

func (c *crdbTester) dropTestDB() error {
	insecure := c.insecureFlag()
	stmt := fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", c.testDBName)
	var args []string
	if len(insecure) > 0 {
		args = []string{"sql", "--url", c.testDBURL, insecure, "--execute", stmt}
	} else {
		args = []string{"sql", "--url", c.testDBURL, "--execute", stmt}
	}
	return c.runCmd("", "cockroach", args...)
}

// teardown executes cleanup tasks when the tests finish running
func (c *crdbTester) teardown() error {
  var err error
  if err = c.dbConn.Close(); err != nil {
    return err
  }
  c.dbConn = nil

  if err = c.dropTestDB(); err != nil {
    return err
  }

  return nil
}

func (c *crdbTester) conn() (*sql.DB, error) {
  if c.dbConn != nil {
    return c.dbConn, nil
  }

  var err error
  c.dbConn, err = sql.Open("postgres", c.testDBURL)
  if err != nil {
    return nil, err
  }

  return c.dbConn, nil
}

func buildQueryString(user, pass, dbname, host string, port int, sslmode, sslkey, sslcert, sslrootcert string) string {
	var up string
	if user != "" {
		up = user
	}
	if pass != "" {
		up = fmt.Sprintf("%s:%s", up, pass)
	}
	output := fmt.Sprintf("postgresql://%s@%s:%d/%s?sslmode=%s", up, host, port, dbname, sslmode)
	if len(sslcert) > 0 {
		output += fmt.Sprintf("&sslcert=%s", sslcert)
	}
	if len(sslkey) > 0 {
		output += fmt.Sprintf("&sslkey=%s", sslkey)
	}
	if len(sslrootcert) > 0 {
		output += fmt.Sprintf("&sslrootcert=%s", sslrootcert)
	}
	return output
}