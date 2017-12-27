package main

import (
    "fmt"
    "os"
    "strconv"
    "encoding/csv"
    "github.com/golang/protobuf/proto"
     zmq "github.com/zeromq/goczmq"
    events "ZmqGo/events"
)


func main() {

   fmt.Println("Started Event Daemon !!")
   inputQueue := make(chan []byte)
   go func(){
       for{
          data := <-inputQueue
          fmt.Println("Received serial data:", data)
          writeDataToFile(data)
       }
   }()

   go listener(inputQueue)
   for {

   }
}


// Listener thread

func listener(inputQ chan []byte) {
    fmt.Println("Started listener thread")
    //'@' at the beginning of source endpoint specifies that this socket has to bind(instead of connect).
    src := "@tcp://127.0.0.1:2000"
    subSock, err := zmq.NewSub(src, "")
    if err != nil {
        fmt.Println("error creating socket:", err)
    }
    defer subSock.Destroy()

    fmt.Println("listening on socket ", src)
    for {
        fmt.Println("receieve frame")
        data, flag, err := subSock.RecvFrame()
        if err != nil {
            fmt.Println("error receiving frame:", err)
        }
        fmt.Println("data, flag", data, flag)
        inputQ <- data
    }
}

func writeDataToFile(binData []byte) {

    file, err := os.OpenFile("eventOut.csv", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
    defer file.Close()
    if err != nil {
        fmt.Println("Error:", err.Error())
        return
    }
    event := new(events.EventOne)
    err = proto.Unmarshal(binData, event)
     if err != nil {
        fmt.Println("Error in unmarshaling data:", err.Error())
        return
    }

    writer := csv.NewWriter(file)
    record := []string{event.GetComponentName(), strconv.Itoa(int(event.GetPriority())), event.GetEventName(), event.GetComments()}
    writer.Write(record)
    fmt.Println(record)

    writer.Flush()

}

