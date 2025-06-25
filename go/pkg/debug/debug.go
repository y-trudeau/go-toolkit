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

   This package provides function supporting debug output. The behavior is 
   trigger by setting the environment variable PTDEBUG to 1. 

*/

package debug 

import (
    "fmt"
    "os"
)

func isdebug() bool {
    if os.Getenv("PTDEBUG") == "1" {
        return true
    } else {
        return false
    }
}
// Print a string message and a variable of any type
func Printvar(msg string, variable any) {
    if ! isdebug() {
        return
    }
    fmt.Fprintf(os.Stderr,"%s: %v\n",msg, variable)
}

// Print a string message
func Print(msg string) {
    if ! isdebug() {
        return
    }
    fmt.Fprintln(os.Stderr,msg)
}
