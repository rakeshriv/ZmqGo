package main

import (
    "fmt"
    "time"
    pub "ZmqGo/publisherSDK"
)

func main() {

    fmt.Println("Running publisher TWO")
    pub.Init("PubTwo")
    time.Sleep(time.Second*1)
    m := map[string]string{
        "EventName":"EventOne",
        "Priority":"5",
        "Comments":"ZMQ example",
    }
    fmt.Println(m)
    err := pub.Publish(m)
    if err != nil {
        fmt.Println("Error: ", err)
    }
    time.Sleep(time.Second*1)

}

