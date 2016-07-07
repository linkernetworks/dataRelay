package main

import (
	"fmt"
	"log"

	sendr "github.com/LinkerNetworks/linkerConnector/sender"
	"github.com/gocql/gocql"
)

func main() {
	// connect to the cluster
	sender := sendr.NewSender("dataRelay")
	sender.SendData("cassandra", "127.0.0.1", "dap", "k2", "v2")

	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "dap"
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal("fail:", err)
	}
	defer session.Close()

	var key, value string

	// if err := session.Query(`INSERT INTO test_topic (key, value) VALUES (?, ?)`, "key1", "v1").Exec(); err != nil {
	// 	log.Fatal(err)
	// }
	iter2 := session.Query("SELECT key,value  FROM test_topic").Iter()
	for iter2.Scan(&key, &value) {
		fmt.Println("kafka:", key, value)
	}
	if err := iter2.Close(); err != nil {
		log.Fatal(err)
	}

	var city string
	var age int
	/* Search for a specific set of records whose 'timeline' column matches
	 * the value 'me'. The secondary index that we created earlier will be
	 * used for optimizing the search */
	if err := session.Query("SELECT city, age FROM users").Consistency(gocql.One).Scan(&city, &age); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Tweet:", city, age)

	iter := session.Query("SELECT city, age  FROM users").Iter()
	for iter.Scan(&city, &age) {
		fmt.Println("Tweet:", city, age)
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
}

// package main

// import (
// 	"bufio"
// 	"fmt"
// 	"io"
// 	"os"
// 	"runtime"
// 	"time"

// 	"github.com/spf13/cobra"
// 	sendr "gihub.com/LinkerNetworks/linkerConnector/sender"
// )

// func main() {
// 	var targetAddr, serverAddr, topic, dest string
// 	var interval int
// 	var usingPipe bool
// 	rootCmd := &cobra.Command{
// 		Use:   "dataRelay",
// 		Short: "relay Kafka data to target DB system",
// 		Run: func(cmd *cobra.Command, args []string) {
// 			if usingPipe {
// 				info, _ := os.Stdin.Stat()

// 				if info.Size() > 0 {
// 					reader := bufio.NewReader(os.Stdin)
// 					processPipe(reader, dest, serverAddr, topic)
// 				}
// 				return
// 			}
// 			if runtime.GOOS != "linux" {
// 				fmt.Println("Collect data from linux for now, application exit.")
// 				return
// 			}

// 			for {
// 				data := NewDataCollector()
// 				procInfo := data.GetProcessInfo()
// 				machineInfo := data.GetMachineInfo()

// 				sendData(dest, serverAddr, topic, "ProcessInfo", procInfo)
// 				sendData(dest, serverAddr, topic, "MachineInfo", machineInfo)

// 				if interval == 0 {
// 					return
// 				}

// 				time.Sleep(time.Millisecond * time.Duration(interval))
// 			}
// 		},
// 	}

// 	rootCmd.Flags().StringVarP(&serverAddr, "server", "s", "", "Kafka server and port")
// 	rootCmd.Flags().StringVarP(&targetAddr, "target", "ts", "", "Target server address and port")
// 	rootCmd.Flags().StringVarP(&topic, "topic", "t", "", "The topic to kafka consume")
// 	rootCmd.Flags().StringVarP(&dest, "dest", "d", "stdout", "Destination to cassandra or kafka")
// 	rootCmd.Execute()

// }

// func processPipe(reader *bufio.Reader, dest, serverAddr, topic string) {
// 	line := 1
// 	for {
// 		input, err := reader.ReadString('\n')
// 		if err != nil && err == io.EOF {
// 			break
// 		}

// 		sendData(dest, serverAddr, topic, "Pipe", input)
// 		line++
// 	}
// }
