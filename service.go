package main

import (
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"strings"
	"sync"
)

type Logger struct {
	//	eventChs []*Event
	mu       sync.Mutex
	eventChs []chan *Event
}

func (log *Logger) AddToEventChs(event *Event) chan *Event {
	resultStream := make(chan *Event, 10)
	resultStream <- event
	return resultStream
}

func (log *Logger) FanIn() chan *Event {
	finalStream := make(chan *Event, 10)
	var wg sync.WaitGroup

	for _, ch := range log.eventChs {
		chCopy := ch
		wg.Add(1)
		go func() {
			defer wg.Done()
			for value := range chCopy {
				finalStream <- value
			}
		}()
	}

	go func() {
		wg.Wait()
		close(finalStream)
	}()

	return finalStream
}

func StartMyMicroservice(ctx context.Context, listenAddr string, aclData string) error {
	var accessData map[string][]string

	err := json.Unmarshal([]byte(aclData), &accessData)
	if err != nil {
		return err
	}

	logger := &Logger{
		eventChs: make([]chan *Event, 10),
	}

	go func() {
		lis, err := net.Listen("tcp", listenAddr)
		if err != nil {
			log.Fatalln("Cant listen port", err)
		}
		server := grpc.NewServer()

		RegisterAdminServer(server, &MyAdminServer{
			aclData: accessData,
			logger:  logger,
		})

		RegisterBizServer(server, &MyBizServer{
			aclData: accessData,
			logger:  logger,
		})

		go server.Serve(lis)

		<-ctx.Done()
		server.Stop()
		err = lis.Close()
		if err != nil {
			return
		}
	}()

	return nil
}

type MyBizServer struct {
	UnimplementedBizServer
	aclData map[string][]string
	logger  *Logger
}

func (myBizServer *MyBizServer) Test(ctx context.Context, nothing *Nothing) (*Nothing, error) {

	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		err := checkAccess(md, myBizServer.aclData, "test")
		if err != nil {
			return nil, err
		}

	} else {
		return nil, status.Error(codes.Unauthenticated, "Unauthenticated")
	}

	events := myBizServer.logger.eventChs
	events = append(events, myBizServer.logger.AddToEventChs(&Event{
		Host:     "127.0.0.1:8080",
		Consumer: md.Get("consumer")[0],
		Method:   "/main.Biz/Test",
	}))

	fmt.Println("AddTests")

	return nothing, nil
}

func (myBizServer *MyBizServer) Check(ctx context.Context, nothing *Nothing) (*Nothing, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		err := checkAccess(md, myBizServer.aclData, "check")
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "Unauthenticated")
		}

	} else {
		return nil, status.Error(codes.Unauthenticated, "Unauthenticated")
	}

	events := myBizServer.logger.eventChs
	fmt.Println("AddCheck")
	events = append(events, myBizServer.logger.AddToEventChs(&Event{
		Host:     "127.0.0.1:8080",
		Consumer: md.Get("consumer")[0],
		Method:   "/main.Biz/Check",
	}))

	return nothing, nil
}

func checkAccess(md metadata.MD, aclData map[string][]string, method string) error {

	consumer := md.Get("consumer")
	if len(consumer) == 0 {
		return status.Error(codes.Unauthenticated, "Unauthenticated")
	}

	methods := aclData[consumer[0]]

	for _, url := range methods {
		url = strings.ToLower(url)
		if strings.HasSuffix(url, "*") || strings.HasSuffix(url, strings.ToLower(method)) {
			return nil
		}
	}
	return status.Error(codes.Unauthenticated, "Unauthenticated")
}

type MyAdminServer struct {
	UnimplementedAdminServer
	aclData map[string][]string
	logger  *Logger
}

func (adminServer *MyAdminServer) Logging(nothing *Nothing, stream Admin_LoggingServer) error {
	ctx := stream.Context()
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		err := checkAccess(md, adminServer.aclData, "logging")
		if err != nil {
			return status.Error(codes.Unauthenticated, "Unauthenticated")
		}
	} else {
		return status.Error(codes.Unauthenticated, "Unauthenticated")
	}
	events := adminServer.logger.eventChs
	events = append(events, adminServer.logger.AddToEventChs(&Event{
		Host:     "127.0.0.1:8080",
		Consumer: md.Get("consumer")[0],
		Method:   "/main.Admin/Logging",
	}))
	fmt.Println("AddToEventChs log logging " + md.Get("consumer")[0])
	streamCh := adminServer.logger.FanIn()
	for event := range streamCh {
		fmt.Println("index")
		err := stream.Send(event)
		if err != nil {
			fmt.Println(err)
		}
	}

	return nil
}

//func (adminServer *MyAdminServer) Statistics(*StatInterval, Admin_StatisticsServer) error
