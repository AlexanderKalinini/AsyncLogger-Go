package main

import (
	"context"
	"encoding/json"
	"google.golang.org/grpc"
	"log"
	"net"
)

func StartMyMicroservice(ctx context.Context, listenAddr string, aclData string) error {

	var accessData map[string][]string

	err := json.Unmarshal([]byte(aclData), &accessData)
	if err != nil {
		return err
	}
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalln("Cant listen port", err)
	}
	server := grpc.NewServer()

	RegisterAdminServer(server, &UnimplementedAdminServer{})

	go func() {
		go server.Serve(lis)
		<-ctx.Done()
		server.Stop()
		lis.Close()
	}()

	return nil
}
