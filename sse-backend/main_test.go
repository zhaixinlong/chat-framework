package main

import (
	"sync"
	"testing"
)

func TestUnregisterRemovesOnlyDisconnectedConnection(t *testing.T) {
	h := NewHub()
	first := h.Register("same")
	second := h.Register("same")
	h.unregisterClient(second)
	h.unregisterClient(second)
	if h.Count() != 1 || h.clients[0] != first {
		t.Fatal("wrong connection removed")
	}
	for range second.messageChan {
	}
	h.HandleMessage("sender", "hello")
	found := false
	for len(first.messageChan) > 0 {
		if (<-first.messageChan).Content == "hello" {
			found = true
		}
	}
	if !found {
		t.Fatal("remaining connection did not receive message")
	}
	h.unregisterClient(first)
	if h.Count() != 0 {
		t.Fatal("connection not removed")
	}
}

func TestConcurrentRegistrationAndBroadcast(t *testing.T) {
	h := NewHub()
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 20; j++ {
				client := h.Register("same")
				h.HandleMessage("same", "message")
				h.Count()
				h.unregisterClient(client)
			}
		}()
	}
	workers.Wait()
	if h.Count() != 0 {
		t.Fatal("connections leaked")
	}
}
