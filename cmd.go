package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sync"
)

var goArg = "%s/loop.sh %s %s %d"

func main() {
	// ./loop -name=2A -c=2 -t=50
	suitName := flag.String("name", "2A", "test suit name to run, e.g: 2A")
	goNum := flag.Int("c", 1, "concurrent goroutines")
	timesNum := flag.Int("t", 1, "test times per goroutines")
	logFile := flag.String("f", "raft.out", "log file")
	dir := flag.String("d", ".", "dir")
	flag.Parse()

	var wg sync.WaitGroup
	wg.Add(*goNum)

	fmt.Printf("%d goroutines, %d times per >>>>>>>\n", *goNum, *timesNum)
	for i := 0; i < *goNum; i++ {
		go func(goIdx int) {
			fmt.Printf(">>goroutine %d: %s\n", goIdx+1, fmt.Sprintf(goArg, *dir, *suitName, *logFile, *timesNum))

			file := fmt.Sprintf("%s.%s.%d", *logFile, *suitName, goIdx)
			cmd := exec.Command("/bin/bash", "-c", fmt.Sprintf(goArg, *dir, *suitName, file, *timesNum))
			bytes, err := cmd.CombinedOutput()
			if err != nil {
				fmt.Printf("error %v; %s, check file %s for details.\n", err, string(bytes), file)
				os.Exit(1)
			}
			fmt.Printf(">>goroutine %d: all success, log file: %s\n", goIdx+1, file)
			wg.Done()
		}(i)
	}
	fmt.Printf("waiting...\n")
	wg.Wait()

	fmt.Printf("All test success, total %d\n", *goNum**timesNum)
}
