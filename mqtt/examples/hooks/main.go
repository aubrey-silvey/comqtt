// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/pubsub"
	"github.com/wind-c/comqtt/v2/mqtt"
	"github.com/wind-c/comqtt/v2/mqtt/hooks/auth"
	"github.com/wind-c/comqtt/v2/mqtt/listeners"
	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

var psTopic pubsub.Topic

func main() {

	// Luke wrote this
	psClient, err := newPubSubClient("substation-data-streams-dev")
	if err != nil {
		log.Fatalf("Unable to build pubsub client")
		os.Exit(1)
	}
	psTopic = *psClient.Topic("substation-incoming-site-dev")

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()

	server := mqtt.New(nil)
	_ = server.AddHook(new(auth.AllowHook), nil)
	tcp := listeners.NewTCP("t1", ":1883", nil)
	err = server.AddListener(tcp)
	if err != nil {
		log.Fatal(err)
	}

	err = server.AddHook(new(ExampleHook), map[string]any{})
	if err != nil {
		log.Fatal(err)
	}

	// Start the server
	go func() {
		err := server.Serve()
		if err != nil {
			log.Fatal(err)
		}
	}()

	// Demonstration of directly publishing messages to a topic via the
	// `server.Publish` method. Subscribe to `direct/publish` using your
	// MQTT client to see the messages.
	// go func() {
	// 	cl := server.NewClient(nil, "local", "inline", true)
	// 	for range time.Tick(time.Second * 1) {
	// 		err := server.InjectPacket(cl, packets.Packet{
	// 			FixedHeader: packets.FixedHeader{
	// 				Type: packets.Publish,
	// 			},
	// 			TopicName: "direct/publish",
	// 			Payload:   []byte("injected scheduled message"),
	// 		})
	// 		if err != nil {
	// 			server.Log.Error("server.InjectPacket", "error", err)
	// 		}
	// 		server.Log.Info("main.go injected packet to direct/publish")
	// 	}
	// }()

	// There is also a shorthand convenience function, Publish, for easily sending
	// publish packets if you are not concerned with creating your own packets.
	// go func() {
	// 	for range time.Tick(time.Second * 5) {
	// 		err := server.Publish("direct/publish", []byte("packet scheduled message"), false, 0)
	// 		if err != nil {
	// 			server.Log.Error("server.Publish", "error", err)
	// 		}
	// 		server.Log.Info("main.go issued direct message to direct/publish")
	// 	}
	// }()

	<-done
	server.Log.Warn("caught signal, stopping...")
	_ = server.Close()
	server.Log.Info("main.go finished")
}

type ExampleHook struct {
	mqtt.HookBase
}

func (h *ExampleHook) ID() string {
	return "events-example"
}

func (h *ExampleHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnConnect,
		mqtt.OnDisconnect,
		mqtt.OnSubscribed,
		mqtt.OnUnsubscribed,
		mqtt.OnPublished,
		mqtt.OnPublish,
	}, []byte{b})
}

func (h *ExampleHook) Init(config any) error {
	h.Log.Info("initialised")
	return nil
}

func (h *ExampleHook) OnConnect(cl *mqtt.Client, pk packets.Packet) error {
	h.Log.Info("client connected", "client", cl.ID)
	return nil
}

func (h *ExampleHook) OnDisconnect(cl *mqtt.Client, err error, expire bool) {
	if err != nil {
		h.Log.Info("client disconnected", "client", cl.ID, "expire", expire, "error", err)
	} else {
		h.Log.Info("client disconnected", "client", cl.ID, "expire", expire)
	}

}

func (h *ExampleHook) OnSubscribed(cl *mqtt.Client, pk packets.Packet, reasonCodes []byte, counts []int) {
	h.Log.Info(fmt.Sprintf("subscribed qos=%v", reasonCodes), "client", cl.ID, "filters", pk.Filters)
}

func (h *ExampleHook) OnUnsubscribed(cl *mqtt.Client, pk packets.Packet, reasonCodes []byte, counts []int) {
	h.Log.Info("unsubscribed", "client", cl.ID, "filters", pk.Filters)
}

func (h *ExampleHook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	h.Log.Info("received from client", "client", cl.ID, "payload", string(pk.Payload))

	pkx := pk
	if string(pk.Payload) == "hello" {
		pkx.Payload = []byte("hello world")
		h.Log.Info("received modified packet from client", "client", cl.ID, "payload", string(pkx.Payload))
	}

	return pkx, nil
}

func (h *ExampleHook) OnPublished(cl *mqtt.Client, pk packets.Packet) {
	h.Log.Info("Recieved Message")

	status := psTopic.Publish(context.Background(), &pubsub.Message{
		Data:       pk.Payload,
		Attributes: map[string]string{"path": pk.TopicName},
	})

	<-status.Ready()
	_, err := status.Get(context.Background())
	if err != nil {
		h.Log.Info("PUBSUB Publish Error:", err.Error())
	}
	h.Log.Info("Message Published")
	// TODO: Check for publish errors.
}

// Luke wrote this
func newPubSubClient(topic string) (*pubsub.Client, error) {
	client, err := pubsub.NewClient(context.Background(), topic)
	return client, err
}
