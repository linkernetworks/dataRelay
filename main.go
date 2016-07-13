package main

// sender := sendr.NewSender("dataRelay")
// sender.SendData("cassandra", "127.0.0.1", "dap", "k2", "v2")

// 	cluster := gocql.NewCluster("127.0.0.1")
// 	cluster.Keyspace = "dap"
// 	cluster.Consistency = gocql.Quorum
// 	session, err := cluster.CreateSession()
// 	if err != nil {
// 		log.Fatal("fail:", err)
// 	}
// 	defer session.Close()

// 	var key, value string

// 	// if err := session.Query(`INSERT INTO test_topic (key, value) VALUES (?, ?)`, "key1", "v1").Exec(); err != nil {
// 	// 	log.Fatal(err)
// 	// }
// 	iter2 := session.Query("SELECT key,value  FROM test_topic").Iter()
// 	for iter2.Scan(&key, &value) {
// 		fmt.Println("kafka:", key, value)
// 	}
// 	if err := iter2.Close(); err != nil {
// 		log.Fatal(err)
// 	}

// 	var city string
// 	var age int
// 	/* Search for a specific set of records whose 'timeline' column matches
// 	 * the value 'me'. The secondary index that we created earlier will be
// 	 * used for optimizing the search */
// 	if err := session.Query("SELECT city, age FROM users").Consistency(gocql.One).Scan(&city, &age); err != nil {
// 		log.Fatal(err)
// 	}
// 	fmt.Println("Tweet:", city, age)

// 	iter := session.Query("SELECT city, age  FROM users").Iter()
// 	for iter.Scan(&city, &age) {
// 		fmt.Println("Tweet:", city, age)
// 	}
// 	if err := iter.Close(); err != nil {
// 		log.Fatal(err)
// 	}
// }

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"

	sendr "github.com/LinkerNetworks/linkerConnector/sender"
	"github.com/Shopify/sarama"

	"github.com/spf13/cobra"
)

var (
	send *sendr.Sender
)

func main() {
	send = sendr.NewSender("dataRelay")
	// sender.SendData("cassandra", "127.0.0.1", "dap", "k2", "v2")

	var targetAddr, serverAddr, topic, dest string
	var usingPipe bool
	rootCmd := &cobra.Command{
		Use:   "dataRelay",
		Short: "relay Kafka data to target DB system",
		Run: func(cmd *cobra.Command, args []string) {
			if usingPipe {
				info, _ := os.Stdin.Stat()

				if info.Size() > 0 {
					reader := bufio.NewReader(os.Stdin)
					processPipe(reader, dest, serverAddr, topic)
				}
				return
			}

			var serverList []string
			serverList = append(serverList, serverAddr)
			c, err := sarama.NewConsumer(serverList, nil)
			if err != nil {
				log.Fatalln(err)
			}

			pc, err := c.ConsumePartition(topic, 0, sarama.OffsetNewest)
			if err != nil {
				log.Fatalln(err)
			}

			go func() {
				signals := make(chan os.Signal, 1)
				signal.Notify(signals, os.Kill, os.Interrupt)
				<-signals
				pc.AsyncClose()
			}()

			for {
				for msg := range pc.Messages() {
					fmt.Printf("Offset: %d\n", msg.Offset)
					fmt.Printf("Key:    %s\n", string(msg.Key))
					fmt.Printf("Value:  %s\n", string(msg.Value))
					fmt.Println()
					send.SendData(sendr.SendDataParam{Dest: dest, SerAddr: targetAddr, Topic: "dap", Key: string(msg.Key), Value: string(msg.Value), Table: "raw_record"})
				}

				if err := c.Close(); err != nil {
					fmt.Println("Failed to close consumer: ", err)
				}
			}
		},
	}

	rootCmd.Flags().BoolVarP(&usingPipe, "pipe", "p", false, "Using pipe mode to forward data")
	rootCmd.Flags().StringVarP(&serverAddr, "source", "s", "", "Kafka server and port")
	rootCmd.Flags().StringVarP(&targetAddr, "target", "g", "", "Target server address and port")
	rootCmd.Flags().StringVarP(&topic, "topic", "t", "", "The topic to kafka consume")
	rootCmd.Flags().StringVarP(&dest, "dest", "d", "stdout", "Destination to cassandra or kafka/spark/cassandra/stdout")
	rootCmd.Execute()

}

func processPipe(reader *bufio.Reader, dest, serverAddr, topic string) {
	line := 1
	for {
		input, err := reader.ReadString('\n')
		if err != nil && err == io.EOF {
			break
		}
		send.SendData(sendr.SendDataParam{Dest: dest, SerAddr: serverAddr, Topic: topic, Key: "Pipe", Value: input})
		line++
	}
}
