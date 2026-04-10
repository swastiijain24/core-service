package worker

// import (
// 	"context"
// 	"fmt"
// 	"log"

// 	"github.com/segmentio/kafka-go"
// )

// func StartWorker(readerA *kafka.Reader, readerB *kafka.Reader) {
// 	for {
// 		msg1 , err := readerA.FetchMessage(context.Background())
// 		if err != nil {
// 			log.Fatal(err)
// 		}

// 		msg2 , err := 

// 		fmt.Printf("Message: %s\n", string(msg.Value))

// 		err = reader.CommitMessages(context.Background(), msg)
// 		if err != nil {
// 			log.Println("commit failed:", err)
// 		}
		
// 	}
// }