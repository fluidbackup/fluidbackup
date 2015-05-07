package main

import "fluidbackup"
import "bufio"
import "strings"
import "os"
import "strconv"
import "fmt"

func main() {
	port := 19838
	if len(os.Args) > 1 {
		tmp, _ := strconv.ParseInt(os.Args[1], 0, 32)
		port = int(tmp)
	}

	fs := fluidbackup.MakeFluidBackup(port)
	reader := bufio.NewReader(os.Stdin)

	for {
		line, _ := reader.ReadString('\n')
		fmt.Printf("Received command: %s\n", line)
		parts := strings.Split(strings.TrimSpace(line), " ")

		if parts[0] == "register" {
			fs.RegisterFile(parts[1])
		} else if parts[0] == "recover" {
			fs.RegisterFile(parts[1])
		} else if parts[0] == "discover" {
			fs.DiscoveredPeer(parts[1])
		}
	}
}
