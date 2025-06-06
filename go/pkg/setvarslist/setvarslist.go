/*
   Copyright 2025, Yves Trudeau, Percona Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at


       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   This package parses a string composed of comma delimited values that can
   also have comma within double quotes. Here's an example of values:

   `innodb_wait_timeout

*/

package setvarslist

import (
	"fmt"
	"github.com/dlclark/regexp2"
)

func regexp2FindAllString(re *regexp2.Regexp, s string) []string {
	var matches []string
	m, _ := re.FindStringMatch(s)
    previous := 0
    fmt.Printf("%v \n",s)
	for m != nil {
        if m.Index > 0 {
            fmt.Printf("p %v I %v   %v\n",previous, m.Index,string(s[previous:m.Index]))
		    matches = append(matches, string(s[previous:m.Index]))
		    //matches = append(matches, m.String())
		    //matches = append(matches, strconv.Itoa(m.Index))
        }
        previous = m.Index + 1
		m, _ = re.FindNextMatch(m)
	}
    if previous < len(s) {
        matches = append(matches, string(s[previous:len(s)]))
    }
	return matches
}

func Getvars(vars string) []string {
//    re := regexp2.MustCompile(`Your RE2-compatible pattern`, regexp2.RE2)
//,               ','
//(?=             look ahead to see if there is:
//(?:             group, but do not capture (0 or more times):
//(?:             group, but do not capture (2 times):
// [^"]*          any character except: '"' (0 or more times)
// "              '"'
//){2}            end of grouping
//)*              end of grouping
// [^"]*          any character except: '"' (0 or more times)
//$               before an optional \n, and the end of the string
//)               end of look-ahead

	re := regexp2.MustCompile(`,(?=(?:(?:[^"]*"){2})*[^"]*$)`,regexp2.RE2)

	return regexp2FindAllString(re,vars)
}
