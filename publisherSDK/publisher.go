package publisherSDK

import (
    "fmt"
    "errors"
    "strconv"
    "reflect"
     _ "ProjZmqGo/events"
     "github.com/golang/protobuf/proto"
     zmq "github.com/zeromq/goczmq"
)


//Structure to keep component's information.

type component struct {
    name string
    dest string
    queue chan []byte //go channel which publisher thread picks messages from
}


var comp *component


// This function is used by the component to register
// itself as a publisher and to set up a publisher thread.

func Init(name string) {

    dest := "127.0.0.1:2000"
    comp = &component{name:name, dest:dest}
    comp.queue = make(chan []byte)
    go publisher(comp.queue)    //Sets up a publisher thread which listens on the channel for any message and publishes it

}


// Publisher thread

func publisher(queue chan []byte) {

    fmt.Println("Starting publisher thread")
    //'<' at the beginning of the destination endpoint tells zmq to connect(instead of bind) to the socket
    dest := ">tcp://"+ comp.dest
    fmt.Println("socket: ", dest)
    pubSock, err := zmq.NewPub(dest)
    if err != nil {
        panic(err)
    }
    defer pubSock.Destroy()
    for {
        data := <-queue
        fmt.Println("Publisher: Sending data", data)
        // FlagNone specifies that there are no more data parts to follow
        err := pubSock.SendFrame(data, zmq.FlagNone)
        if err != nil {
            fmt.Println("error sending frame, Error :", err)
        }
    }
}


// This function is used to publish the event. Input to this method is a
// map containing field and it's value in the form of a key:value pair.

func Publish(propMap map[string]string) error{

    var eventName string
    var err error
    var ok bool
    var regEvent reflect.Type
    err = nil
    if eventName, ok = propMap["EventName"]; !ok {
        err = errors.New("Event name not specified")
        return err
    }
    eventName = "events." + eventName
    fmt.Println("event name", eventName)

    regEvent = proto.MessageType(eventName)
    if regEvent == nil {
        return fmt.Errorf("Event %s not registered", eventName)
    }

    eventPtr := reflect.New(regEvent.Elem())
    event := eventPtr.Elem()
    eventType := event.Type()

    propMap["ComponentName"] = comp.name

    for key, value := range propMap {
        fmt.Printf("Key:%s,Value:%s\n", key,value)
        field := event.FieldByName(key)
        val := reflect.ValueOf(value)

        if !field.IsValid() {
            return fmt.Errorf("No such field '%s' in event '%s'", key, eventName)
        }

        // If obj field value is not settable an error is thrown
        if !field.CanSet() {
            return fmt.Errorf("Field '%s' is unsettable", key)
        }

        if field.Type() != val.Type() {
            if(key == "Priority"){
                valInt, _ := strconv.Atoi(value)
                valInt32  := int32(valInt)
                val = reflect.ValueOf(valInt32)
            }else {
                return fmt.Errorf("Provided value doesn't match '%s' field's type", key)
            }
        }
        field.Set(val)
    }

    //TO-DO : remove some of these debug statements later
    fmt.Println("After setting")
    for i:=0; i < event.NumField(); i++ {
        f := event.Field(i)
        fmt.Printf("%s:%s\n", eventType.Field(i).Name, f.Interface())
    }

    eventInterface := eventPtr.Interface().(proto.Message)
    serialData, err := proto.Marshal(eventInterface)
    if err != nil {
        fmt.Println("Error marshaling data, Error:", err)
        return err
    }

    fmt.Println("serial data", serialData)
    comp.queue <- serialData

    return err
}


