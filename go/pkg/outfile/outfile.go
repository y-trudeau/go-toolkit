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

   Outfile writes rows to a file in SELECT INTO OUTFILE format. See
   https://dev.mysql.com/doc/refman/8.0/en/load-data.html  for more
   details

*/

package outfile

import (
	// "errors"
	// "fmt"
	// "regexp"
	// "strconv"
	// "strings"
	"bufio"
)

type OutfileDest struct {
	OutWriter bufio.Writer
}

func (OutfileDest *ow) Write(v string) []string {

	// 	my ( $self, $fh, $rows ) = @_;
	//    foreach my $row ( @$rows ) {
	//       print $fh escape($row), "\n"
	//          or die "Cannot write to outfile: $OS_ERROR\n";
	//    }
	//    return;

}

func escape(v string) []string {

	//    my ( $row ) = @_;
	//    return join("\t", map {
	//       s/([\t\n\\])/\\$1/g if defined $_;  # Escape tabs etc
	//       defined $_ ? $_ : '\N';             # NULL = \N
	//    } @$row);

}
