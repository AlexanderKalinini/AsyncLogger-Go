package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"io"
	"log"
	"net"
	"strings"
	"sync"
)

type Logger struct {
	//	eventChs []*Event
	mu       sync.Mutex
	eventChs map[chan *Event]struct{}
}

func (log *Logger) Subscribe() chan *Event {
	log.mu.Lock()
	defer log.mu.Unlock()
	newCh := make(chan *Event, 10)
	log.eventChs[newCh] = struct{}{}
	return newCh
}

func (log *Logger) Unsubscribe(ch chan *Event) {
	log.mu.Lock()
	defer log.mu.Unlock()
	delete(log.eventChs, ch)
	close(ch)
}

func (log *Logger) Log(event *Event) {
	log.mu.Lock()
	defer log.mu.Unlock()
	for ch := range log.eventChs {
		ch <- event
	}
}

func (log *Logger) FanIn() chan *Event {
	finalStream := make(chan *Event, 10)
	var wg sync.WaitGroup

	for ch := range log.eventChs {
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
	var logger = &Logger{
		eventChs: make(map[chan *Event]struct{}),
	}
	err := json.Unmarshal([]byte(aclData), &accessData)
	if err != nil {
		return err
	}

	go func() {
		listener, err := net.Listen("tcp", listenAddr)
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

		go server.Serve(listener)

		<-ctx.Done()
		server.Stop()
		err = listener.Close()
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

	myBizServer.logger.Log(&Event{
		Host:     "127.0.0.1:8080",
		Consumer: md.Get("consumer")[0],
		Method:   "/main.Biz/Test",
	})

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

	myBizServer.logger.Log(&Event{
		Host:     "127.0.0.1:8080",
		Consumer: md.Get("consumer")[0],
		Method:   "/main.Biz/Check",
	})

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
	mu sync.Mutex
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

	adminServer.logger.Log(&Event{
		Host:     "127.0.0.1:8080",
		Consumer: md.Get("consumer")[0],
		Method:   "/main.Admin/Logging",
	})
	eventCh := adminServer.logger.Subscribe()
	defer adminServer.logger.Unsubscribe(eventCh)
	for event := range eventCh {
		err := stream.Send(event)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				fmt.Println("Client closed connection")
				return nil
			}
			fmt.Println(err)
		}
	}

	return nil
}

//func (adminServer *MyAdminServer) Statistics(*StatInterval, Admin_StatisticsServer) error
