package main

import "context"

func StartMyMicroservice(ctx context.Context, listenAddr string, aclData string) error {

	select {
	case <-ctx.Done():

	default:
	}

	return nil
}
