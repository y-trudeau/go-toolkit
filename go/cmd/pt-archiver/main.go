/*
   Copyright 2023, Yves Trudeau, Percona Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at


       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   This package handles the manipulation of the MySQL version number. It uses
   the nomenclature of:
   https://docs.percona.com/percona-server/8.0/server-version-numbers.html

*/

package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/y-trudeau/go-toolkit/go/dsn"
)

var bDebug = false

type Configuration struct {
	Analyze         string // Run ANALYZE TABLE afterwards on --source (s) and/or --dest (d).
	AscendFirst     bool   // Ascend only first column of index.
	AskPass         bool   // Prompt for a password when connecting to MySQL.
	Buffer          bool   // Buffer output to --file and flush at commit.
	BulkDelete      bool   // Delete each chunk with a single statement (implies --commit-each).
	BulkDeleteLimit bool   // Add --limit to --bulk-delete statement
	BulkInsert      bool   // Insert each chunk with LOAD DATA INFILE (implies --bulk-delete --commit-each).
	CheckColumns    bool   // Ensure --source and --dest have same columns.
	CheckTime       int    // If --check-slave-lag is given, this defines how long the tool pauses (in seconds) each time it discovers
	// that a slave is lagging. This check is performed every 100 rows.
	CheckSlaveLag string // Pause archiving until the specified DSN's slave lag is less than --max-lag.
	// Multiple DSN can be provided when seperated by ';'
	Columns      string // Comma-separated list of columns to archive.
	CommitEach   bool   // Commit each set of fetched and archived rows (disables --txn-size).
	Dest         string // DSN specifying the table to archive to
	DryRun       bool   // Print queries and exit without doing anything
	File         string // File to archive to, with DATE_FORMAT()-like formatting.
	ForUpdate    bool   // Adds the FOR UPDATE modifier to SELECT statements.
	Header       bool   // Print column header at top of --file.
	Ignore       bool   // Use IGNORE for INSERT statements.
	Limit        int    // Number of rows to fetch and archive per statement.
	Local        bool   // Do not write OPTIMIZE or ANALYZE queries to binlog.
	MaxFlowCtl   int    // Max Percentage of time spent doing flow
	MaxLag       int    // Pause archiving if the slave given by --check-slave-lag lag(s) Default: 1
	NoAscend     bool   // Do not use acending index optimization
	NoDelete     bool   // Do not delete the archived rows
	Optimize     string // Run OPTIMIZE TABLE afterwards on --source (s) and/or --dest (d)
	OutputFormat string // Used with --file to specify the output format
	// Valid formats are:
	// dump: MySQL dump format using tabs as field separator (default)
	// csv : Dump rows using ',' as separator and optionally enclosing fields by '"'.
	//		This format is equivalent to FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'. `)
	Pid            string        // Create the given PID file.
	Plugin         string        // Path of Golang .so library to use as plugin (see: https://pkg.go.dev/plugin)
	PrimaryKeyOnly bool          // Primary key columns only
	Progress       int           // Print progress information every X rows
	Purge          bool          // Purge instead of archiving
	Quiet          bool          // Do not print any output, such as for --statistics.
	Replace        bool          // Causes INSERTs into --dest to be written as REPLACE.
	Retries        int           // Number of retries per timeout or deadlock.
	RunTime        time.Duration // Time to run before exiting in golang time.Duration format.
	NoSafeAutoInc  bool          // Disable the auto-increment safety checks.
	ExitSentinel   string        // Exit if the file exists.
	PauseSentinel  string        // Pause if the file exists.
	SlaveUser      string        // Sets the user to be used to connect to the slaves.
	SlavePassword  string        // Sets the password to be used to connect to the slaves.
	SetVars        string        // Set the MySQL variables in this comma-separated list of variable=value pairs.
	ShareLock      bool          // Adds the LOCK IN SHARE MODE modifier to SELECT statements.
	SkipFKChecks   bool          // Disables foreign key checks with SET FOREIGN_KEY_CHECKS=0.
	SleepTime      time.Duration // Time to sleep between fetches in golang time.Duration format.
	SleepCoef      float64       // Calculate --sleep as a multiple of the last SELECT time
	Source         string        // DSN specifying the table to archive from.
	Statistics     bool          // Collect and print timing statistics.
	Stop           string        // Stop running instances by creating the sentinel file.
	TxnSize        int           // Number of rows per transaction (default = 1).
	Version        bool          // Print the version and exit.
	Where          string        // WHERE clause to limit which rows to archive (required).
	WhyQuit        bool          // Print reason for exiting unless rows exhausted.

}

var Config Configuration

func (config *Configuration) init() {
	defaultZeroTime, _ := time.ParseDuration("0")

	flag.StringVar(&config.Analyze, "analyze", "", "Run ANALYZE TABLE afterwards on --source and/or --dest.")
	flag.BoolVar(&config.AscendFirst, "ascent-first", false, "Ascend only first column of index.")
	flag.BoolVar(&Config.AskPass, "ask-pass", false, "Prompt for a password when connecting to MySQL.")
	flag.BoolVar(&Config.Buffer, "buffer", false, "Buffer output to --file and flush at commit.")
	flag.BoolVar(&Config.BulkDelete, "bulk-delete", false, "Delete each chunk with a single statement (implies --commit-each).")
	flag.BoolVar(&Config.BulkDeleteLimit, "bulk-delete-limit", true, "Add --limit to --bulk-delete statement")
	flag.BoolVar(&Config.BulkInsert, "bulk-insert", false, "Insert each chunk with LOAD DATA INFILE (implies --bulk-delete --commit-each).")
	flag.BoolVar(&Config.CheckColumns, "check-columns", true, "Ensure --source and --dest have same columns.")
	flag.IntVar(&Config.CheckTime, "check-interval", 1, `If --check-slave-lag is given, this defines how long the tool pauses (in seconds) each time it discovers
   that a slave is lagging. This check is performed every 100 rows.`)
	flag.StringVar(&Config.CheckSlaveLag, "check-slave-lag", "", `Pause archiving until the specified DSN's slave lag is less than --max-lag.
   Multiple DSN can be provided when seperated by ';'`)
	flag.StringVar(&Config.Columns, "columns", "", "Comma-separated list of columns to archive.")
	flag.BoolVar(&Config.CommitEach, "commit-each", false, "Commit each set of fetched and archived rows (disables --txn-size).")
	flag.StringVar(&Config.Dest, "dest", "", "DSN specifying the table to archive to.")
	flag.BoolVar(&Config.DryRun, "dry-run", false, "Print queries and exit without doing anything.")
	flag.StringVar(&Config.File, "file", "", "File to archive to, with DATE_FORMAT()-like formatting.")
	flag.BoolVar(&Config.ForUpdate, "for-update", false, "Adds the FOR UPDATE modifier to SELECT statements.")
	flag.BoolVar(&Config.Header, "header", false, "Print column header at top of --file.")
	flag.BoolVar(&Config.Ignore, "ignore", false, "Use IGNORE for INSERT statements.")
	flag.IntVar(&Config.Limit, "limit", 1, "Number of rows to fetch and archive per statement.")
	flag.BoolVar(&Config.Local, "local", false, "Do not write OPTIMIZE or ANALYZE queries to binlog.")
	flag.IntVar(&Config.MaxFlowCtl, "max-flow-ctl", 1, "Number of rows to fetch and archive per statement.")
	flag.IntVar(&Config.MaxLag, "max-lag", 1, "Pause archiving if the slave given by --check-slave-lag lags.). Default: 1s")
	flag.BoolVar(&Config.NoAscend, "no-ascend", false, "Do not use acending index optimization")
	flag.BoolVar(&Config.NoDelete, "no-delete", false, "Do not delete the archived rows")
	flag.StringVar(&Config.Optimize, "optimize", "", "Run OPTIMIZE TABLE afterwards on --source and/or --dest")
	flag.StringVar(&Config.OutputFormat, "output-format", "dump", `Used with --file to specify the output format.

   Valid formats are:
   dump: MySQL dump format using tabs as field separator (default)
   csv : Dump rows using ',' as separator and optionally enclosing fields by '"'.
         This format is equivalent to FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'. `)
	flag.StringVar(&Config.Pid, "pid", "", "Create the given PID file.")
	flag.StringVar(&Config.Plugin, "plugin", "", "Golang .so library to use as plugin.") // https://pkg.go.dev/plugin
	flag.BoolVar(&Config.PrimaryKeyOnly, "primary-key-only", false, "Primary key columns only.")
	flag.IntVar(&Config.Progress, "progress", 0, "Print progress information every X rows.")
	flag.BoolVar(&Config.Purge, "purge", false, "Purge instead of archiving.")
	flag.BoolVar(&Config.Quiet, "quiet", false, "Do not print any output, such as for --statistics.")
	flag.BoolVar(&Config.Replace, "Replace", false, "Causes INSERTs into --dest to be written as REPLACE.")
	flag.IntVar(&Config.Retries, "retry", 1, "Number of retries per timeout or deadlock.")
	flag.DurationVar(&Config.RunTime, "run-time", defaultZeroTime, "Time to run before exiting in golang time.Duration format.")
	flag.BoolVar(&Config.NoSafeAutoInc, "no-safe-auto-increment", false, "Disable the auto-increment safety checks.")
	flag.StringVar(&Config.ExitSentinel, "exit-sentinel", "", "Exit if the file exists.")
	flag.StringVar(&Config.PauseSentinel, "pause-sentinel", "", "Pause if the file exists.")
	flag.StringVar(&Config.SlaveUser, "slave-user", "", "Sets the user to be used to connect to the slaves.")
	flag.StringVar(&Config.SlavePassword, "slave-password", "", "Sets the password to be used to connect to the slaves.")
	flag.StringVar(&Config.SetVars, "set-vars", "", "Set the MySQL variables in this comma-separated list of variable=value pairs.")
	flag.BoolVar(&Config.ShareLock, "share-lock", false, "Adds the LOCK IN SHARE MODE modifier to SELECT statements.")
	flag.BoolVar(&Config.SkipFKChecks, "skip-foreign-key-checks", false, "Disables foreign key checks with SET FOREIGN_KEY_CHECKS=0.")
	flag.DurationVar(&Config.SleepTime, "sleep", defaultZeroTime, "Time to sleep between fetches in golang time.Duration format.")
	flag.Float64Var(&Config.SleepCoef, "sleep-coef", 0.0, "Calculate --sleep as a multiple of the last SELECT time")
	flag.StringVar(&Config.Source, "source", "", "DSN specifying the table to archive from.")
	flag.BoolVar(&Config.Statistics, "statistics", false, "Collect and print timing statistics.")
	flag.StringVar(&Config.Stop, "stop", "", "Stop running instances by creating the sentinel file.")
	flag.IntVar(&Config.TxnSize, "txn-size", 1, "Number of rows per transaction (default = 1).")
	flag.BoolVar(&Config.Version, "version", false, "Show version and exit.")
	flag.StringVar(&Config.Where, "where", "", "WHERE clause to limit which rows to archive (required).")
	flag.BoolVar(&Config.WhyQuit, "why-quit", false, "Print reason for exiting unless rows exhausted.")

}

func (config *Configuration) Print() {
	fmt.Printf("Parameters read from the command line or at their default values:\n")
	fmt.Printf("analyze is set to: '%v'\n", config.Analyze)
	fmt.Printf("ascent-first is set to: %v\n", config.AscendFirst)
	fmt.Printf("ask-pass is set to: %v\n", config.AskPass)
	fmt.Printf("buffer is set to: %v\n", config.Buffer)
	fmt.Printf("bulk-delete is set to: %v\n", config.BulkDelete)
	fmt.Printf("bulk-delete-limit is set to: %v\n", config.BulkDeleteLimit)
	fmt.Printf("bulk-insert is set to: %v\n", config.BulkInsert)
	fmt.Printf("check-columns is set to: %v\n", config.CheckColumns)
	fmt.Printf("check-slave-lag is set to: '%v'\n", config.CheckSlaveLag)
	fmt.Printf("check-time is set to: %v\n", config.CheckTime)
	fmt.Printf("columns is set to: '%v'\n", config.Columns)
	fmt.Printf("commit-each is set to: %v\n", config.CommitEach)
	fmt.Printf("dest is set to: '%v'\n", config.Dest)
	fmt.Printf("dry-run is set to: %v\n", config.DryRun)
	fmt.Printf("exit-sentinel is set to: '%v'\n", config.ExitSentinel)
	fmt.Printf("file is set to: '%v'\n", config.File)
	fmt.Printf("for-update is set to: %v\n", config.ForUpdate)
	fmt.Printf("header is set to: %v\n", config.Header)
	fmt.Printf("ignore is set to: %v\n", config.Ignore)
	fmt.Printf("limit is set to: %v\n", config.Limit)
	fmt.Printf("local is set to: %v\n", config.Local)
	fmt.Printf("max-flow-ctl is set to: %v\n", config.MaxFlowCtl)
	fmt.Printf("max-lag is set to: %v\n", config.MaxLag)
	fmt.Printf("no-ascend is set to: %v\n", config.NoAscend)
	fmt.Printf("no-delete is set to: %v\n", config.NoDelete)
	fmt.Printf("no-safe-auto-increment is set to: %v\n", config.NoSafeAutoInc)
	fmt.Printf("optimize is set to: '%v'\n", config.Optimize)
	fmt.Printf("output-format is set to: %v\n", config.OutputFormat)
	fmt.Printf("pause-sentinel is set to: '%v'\n", config.PauseSentinel)
	fmt.Printf("pid is set to: '%v'\n", config.Pid)
	fmt.Printf("plugin is set to: '%v'\n", config.Plugin)
	fmt.Printf("primary-key-only is set to: %v\n", config.PrimaryKeyOnly)
	fmt.Printf("progress is set to: %v\n", config.Progress)
	fmt.Printf("purge is set to: %v\n", config.Purge)
	fmt.Printf("quiet is set to: %v\n", config.Quiet)
	fmt.Printf("replace is set to: %v\n", config.Replace)
	fmt.Printf("retries is set to: %v\n", config.Retries)
	fmt.Printf("run-time is set to: %v\n", config.RunTime)
	fmt.Printf("slave-password is set to: '%v'\n", config.SlavePassword)
	fmt.Printf("slave-user is set to: '%v'\n", config.SlaveUser)
	fmt.Printf("set-vars is set to: '%v'\n", config.SetVars)
	fmt.Printf("share-lock is set to: %v\n", config.ShareLock)
	fmt.Printf("skip-foreign-key-checks is set to: %v\n", config.SkipFKChecks)
	fmt.Printf("sleep is set to: %v\n", config.SleepTime)
	fmt.Printf("sleep-coef is set to: %v\n", config.SleepCoef)
	fmt.Printf("source is set to: '%v'\n", config.Source)
	fmt.Printf("statistics is set to: %v\n", config.Statistics)
	fmt.Printf("stop is set to: '%v'\n", config.Statistics)
	fmt.Printf("txn-size is set to: %v\n", config.TxnSize)
	fmt.Printf("version is set to: %v\n", config.Version)
	fmt.Printf("where is set to: '%v'\n", config.Where)
	fmt.Printf("why-quit is set to: %v\n", config.WhyQuit)

}

func (config *Configuration) Validate() error {

	// Parameters validation logic
	// output-format allows ['dump'|'csv']
	if len(config.OutputFormat) > 0 {
		if !(config.OutputFormat == "dump" || config.OutputFormat == "csv") {
			return fmt.Errorf("Allowed values for --output-format are 'dump' or 'csv'")
		}
	}
	// analyze and optimize allow ['d'|'s'|'ds']
	if len(config.Analyze) > 0 {
		if !(config.Analyze == "d" || config.Analyze == "s" || config.Analyze == "ds") {
			return fmt.Errorf("Allowed values for --analyze are 'd' or 's' and 'ds'")
		}
	}

	if len(config.Optimize) > 0 {
		if !(config.Optimize == "d" || config.Optimize == "s" || config.Optimize == "ds") {
			return fmt.Errorf("Allowed values for --optimize are 'd' or 's' and 'ds'")
		}
	}

	// DSNs must have valid fields: source, dest, check-slaves
	if len(config.Source) > 0 {
		if dsn.Validate(config.Source) != nil {
			return fmt.Errorf("Source is not a valid DSN: '%v'",config.Source)
		}
	}

	if len(config.Dest) > 0 {
		if dsn.Validate(config.Dest) != nil {
			return fmt.Errorf("Dest is not a valid DSN: '%v'",config.Dest)
		}
	}

	if len(config.CheckSlaveLag) > 0 {
		if dsn.Validate(config.CheckSlaveLag) != nil {
			return fmt.Errorf("CheckSlaveLag is not a valid DSN: '%v'",config.CheckSlaveLag)
		}
	}

	// set-vars must be a comma separated list of 'variable1=value1,variable2='value2=123' pairs
	if len(config.SetVars) > 0 {
		re := regexp.MustCompile(`[a-z_]*=('[a-z_\-=, ]*'|[a-z]*),?`)
		if !re.MatchString(config.SetVars) {
			return fmt.Errorf("Incorrectly formatted 'set-vars', must match `[a-z_]*=('[a-z_\-=, ]*'|[a-z]*),?`")
		}

	}
	// where must be set
	if len(config.Where) == 0 {
		return fmt.Errorf("'Where' must be set")
	}
	
	// integers all need to be positive (no negative values makes sense)
	if config.CheckTime < 0 {
		return fmt.Errorf("'check-time' must be zero or positive")
	}
	if config.Limit < 0 {
		return fmt.Errorf("'limit' must be zero or positive")
	}
	if config.MaxFlowCtl < 0 {
		return fmt.Errorf("'max-flow-ctl' must be zero positive")
	}
	if config.Progress < 0 {
		return fmt.Errorf("'progress' must be zero positive")
	}
	if config.Retries < 0 {
		return fmt.Errorf("'retries' must be zero positive")
	}
	if config.TxnSize < 0 {
		return fmt.Errorf("'txn-size' must be zero positive")
	}

	// All good so nil
	return nil
}


func (config *Configuration) Usage() error {

	return nil
}

func main() {

	// is PTDEBUG environment variable set to 1?
	if os.Getenv("PTDEBUG") == "1" {
		fmt.Printf("Environment variable PTDEBUG is set to 1")
		bDebug = true
	}

	Config.init()

	visitor := func(a *flag.Flag) {
		fmt.Println(" --"+a.Name, "  "+a.Usage, "(Default: ", a.Value, ")")
	}
	// Override Usage to get more details
	flag.Usage = func() {
		fmt.Print(
			`
Usage: pt-archiver [OPTIONS] --source DSN --where WHERE

pt-archiver nibbles records from a MySQL table.  The --source and --dest
arguments use DSN syntax; if COPY is yes, --dest defaults to the key's value
from --source.

Examples:

Archive all rows from oltp_server to olap_server and to a file:

  pt-archiver --source h=oltp_server,D=test,t=tbl --dest h=olap_server \
    --file '/var/log/archive/%Y-%m-%d-%D.%t'                           \
    --where "1=1" --limit 1000 --commit-each

Purge (delete) orphan rows from child table:

  pt-archiver --source h=host,D=db,t=child --purge \
    --where 'NOT EXISTS(SELECT * FROM parent WHERE col=child.col)'
			
 --help  Print this help message `)

		flag.VisitAll(visitor)
	}

	// Load the command line flags
	flag.Parse()

	//
	if bDebug {
		Config.Print()
	}

	if Config.Version {
		fmt.Printf("Version 0.1\n")
		os.Exit(0)
	}

	err := Config.Validate()
	if err != nil {
		fmt.Printf("Error validating the command line arguments: %v", err)
		os.Exit(1)
	}
}
