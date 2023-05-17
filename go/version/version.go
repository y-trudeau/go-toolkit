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

package version

import (
	"strings"
)

// type MysqlVersion struct {
// 	version string
// }

func splitVersion(v string) []string {

}

func validate(v string) bool {
	// a typical version number is like 8.0.29-21.3
	// 8.0.29 is the base version
	// The base version is made of digits and two '.'
	// Major version is 8.0
	// Minor version is 29

	// -21.3 is the build info
	// The build info can have letters but no punctuation other than [.-]

	dashPos := strings.Index(v, '-')

}

func Major(v string) uint16 {
	return 1
}

func Minor(v string) uint8 {
	return 1
}

func Build(v string) string {
	return "1"
}

func Normalized(v string) uint32 {
	return 1
}

func Compare(v1 string, v2 string) int8 {

	return 0
}
