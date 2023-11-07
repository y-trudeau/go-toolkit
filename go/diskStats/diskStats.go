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

	This package implements most of the logic in the old shell pt-diskstats;
    it parses data from /proc/diskstats, calculcates deltas, and prints those.

*/
package diskstats

var max_line uint8
var printed_lines uint8

