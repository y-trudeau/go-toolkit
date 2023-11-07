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
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// split a version in parts,
// v is the version string and is assumed to have been validated
func splitVersion(v string) []string {
	rel := ""
	// first, we identify if there is a release part with '-'
	idxRel := strings.Index(v, "-")
	if idxRel == -1 {
		idxRel = len(v)
		rel = ""
	} else {
		rel = v[idxRel+1:]
	}
	splitVer := strings.Split(v[:idxRel], ".")
	splitVer = append(splitVer, rel)

	return splitVer

}

// Validate a version string to see if it conforms to the regular format
func Validate(v string) bool {
	// a typical version number is like 8.0.29-21.3
	// 8.0.29 is the base version
	// The base version is made of digits and two '.'
	// Major version is 8.0
	// Minor version is 29

	// -21.3 is the build info
	// The build info can have letters but no punctuation other than [.-]

	re := regexp.MustCompile("^([58])\x2e([0-9])\x2e([0-9][0-9]$|[0-9]$|[0-9][0-9]-.*$|[0-9]-.*$)")
	return re.MatchString(v)

}

func Major(v string) (string, error) {
	if !Validate(v) {
		err := errors.New("invalid version format")
		return "", err
	}

	vParts := splitVersion(v)

	return vParts[0], nil
}

func Minor(v string) (string, error) {
	if !Validate(v) {

		return "", errors.New("invalid version format")
	}

	vParts := splitVersion(v)

	return vParts[1] + "." + vParts[2], nil

}

func Release(v string) (string, error) {
	if !Validate(v) {
		return "", errors.New("invalid version format")
	}

	vParts := splitVersion(v)
	return "-" + vParts[3], nil
}

// Returns the normalized form of the version number to ease comparison
// 8.0.30 becomes 80030
func Normalized(v string) (string, error) {

	if !Validate(v) {
		return "", errors.New("invalid version format")
	}

	vParts := splitVersion(v)
	digit1, _ := strconv.Atoi(vParts[0])
	digit2, _ := strconv.Atoi(vParts[1])
	digit3, _ := strconv.Atoi(vParts[2])

	return fmt.Sprintf("%d%02d%02d", digit1, digit2, digit3), nil
}

// Compare two version strings
// -1: v1 is older than v2
//  0: v1 is same as v2
//  1: v1 is younger than v2
func Compare(v1 string, v2 string) (int8, error) {

	if !Validate(v1) {
		return 0, errors.New("invalid version format")
	}

	if !Validate(v2) {
		return 0, errors.New("invalid version format")
	}

	normV1, _ := Normalized(v1)
	normV2, _ := Normalized(v2)

	if normV1 < normV2 {
		return -1, nil
	}

	if normV1 > normV2 {
		return 1, nil
	}

	return 0, nil

}
