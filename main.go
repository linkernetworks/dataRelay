package main

import (
	"bufio"
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
					log.Printf("Offset: %d data entry \n", msg.Offset)
					send.SendData(sendr.SendDataParam{Dest: dest, SerAddr: targetAddr, Topic: "dap", Key: string(msg.Key), Value: string(msg.Value), Table: "raw_record"})
				}

				if err := c.Close(); err != nil {
					log.Println("Failed to close consumer: ", err)
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
