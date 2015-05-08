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
		fmt.Printf("[UI] Received command: %s\n", line)
		parts := strings.Split(strings.TrimSpace(line), " ")

		if parts[0] == "register" {
			if len(parts) >= 2 {
				fs.RegisterFile(parts[1])
			} else {
				fmt.Println("[UI] usage: register filename")
			}
		} else if parts[0] == "recover" {
			if len(parts) >= 2 {
				fs.RecoverFile(parts[1])
			} else {
				fmt.Println("[UI] usage: recover filename")
			}
		} else if parts[0] == "discover" {
			if len(parts) >= 2 {
				fs.DiscoveredPeer(parts[1])
			} else {
				fmt.Println("[UI] usage: discover ipaddr:port (e.g., discover 127.0.0.1:19838)")
			}
		} else if parts[0] == "set" {
			if len(parts) >= 3 {
				n, _ := strconv.ParseInt(parts[1], 0, 32)
				k, _ := strconv.ParseInt(parts[2], 0, 32)
				fs.SetReplication(int(n), int(k))
			} else {
				fmt.Println("[UI] usage: set n k (e.g. set 12 8)")
			}
		} else if parts[0] == "quit" || parts[0] == "stop" || parts[0] == "exit" {
			fs.Stop()
			break
		} else if parts[0] == "save" {
			fs.Save()
		} else if parts[0] == "load" {
			fs.Load()
		}
	}
}
