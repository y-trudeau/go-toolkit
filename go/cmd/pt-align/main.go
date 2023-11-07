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
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"
)

func main() {

	var data []string

	bVersionFlagPtr := flag.Bool("version", false, "Print the version")

	visitor := func(a *flag.Flag) {
		fmt.Println(" --"+a.Name, a.Usage, "(Default: ", a.Value, ")")
	}
	// Override Usage to get more details
	flag.Usage = func() {
		fmt.Printf("pt-align reads lines and splits them into words.  It counts how many\n")
		fmt.Printf("words each line has, and if there is one number that predominates, it assumes\n")
		fmt.Printf("this is the number of words in each line.  Then it discards all lines that\n")
		fmt.Printf("don't have that many words, and looks at the 2nd line that does.  It assumes\n")
		fmt.Printf("this is the first non-header line.  Based on whether each word looks numeric\n")
		fmt.Printf("or not, it decides on column alignment.  Finally, it goes through and decides\n")
		fmt.Printf("how wide each column should be, and then prints them out.\n\n")
		fmt.Printf("This is useful for things like aligning the output of vmstat or iostat so it\n")
		fmt.Printf("is easier to read.\n\n")
		fmt.Printf(" --help Print this help\n")

		flag.VisitAll(visitor)
	}

	flag.Parse()

	if *bVersionFlagPtr {
		fmt.Printf("Version 0.1\n")
	}

	//fmt.Printf("NArg %v  Args %v\n", flag.NArg(), flag.Args())

	// Open either a file or stdin
	var scanner *bufio.Scanner

	if flag.NArg() > 0 {
		//fmt.Printf("Reading from %v\n", flag.Args()[1])
		file, err := os.Open(flag.Args()[1])
		if err != nil {
			panic(err)
		}
		defer file.Close()

		scanner = bufio.NewScanner(file)
	} else {
		//fmt.Printf("Reading from os.Stdin\n")
		scanner = bufio.NewScanner(os.Stdin)
	}

	for scanner.Scan() {
		data = append(data, scanner.Text())
	}

	// Do we have enough lines to work on
	if len(data) < 2 {
		fmt.Println("I need at least 2 lines")
		os.Exit(1)
	}

	// At this point, data is a slice of the content, line by line.
	// Now, we need to split all lines around spaces and tabs
	var colWidth []int

	for line := 0; line < len(data); line++ {
		words := strings.Fields(data[line]) // likely will need FieldsFunc
		//fmt.Printf("words %v\n", words)
		//fmt.Printf("len(words) %v\n", len(words))

		for w := 0; w < len(words); w++ {
			//fmt.Printf("w %v\n", w)
			//fmt.Printf("words[w] %v\n", words[w])
			if (w + 1) > len(colWidth) {
				//fmt.Printf("Append to colWidth\n")
				colWidth = append(colWidth, utf8.RuneCountInString(words[w]))
				continue
			}

			if colWidth[w] < utf8.RuneCountInString(words[w]) {
				colWidth[w] = utf8.RuneCountInString(words[w])
			}
			//fmt.Printf("colWidth %v\n", colWidth)
		}
	}

	// Now we can print each line
	for line := 0; line < len(data); line++ {
		// Create the printf format string
		words := strings.Fields(data[line])

		FormatedLine := ""
		for i := 0; i < len(words); i++ {
			printFormat := "%" + strconv.Itoa(colWidth[i]) + "s "
			FormatedLine += fmt.Sprintf(printFormat, words[i])
		}

		fmt.Printf("%s\n", FormatedLine)
	}
}
