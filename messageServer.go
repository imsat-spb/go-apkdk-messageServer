package messageServer

import (
	"bytes"
	"fmt"
	"github.com/imsat-spb/go-apkdk-core"
	"github.com/streadway/amqp"
	"time"
)

type Server struct {
	serverAddress    string
	logger           core.Logger
	messageServerUrl string
	exchangeName     string
}

type Configuration struct {
	ServerHostName string
	ServerPort     int
	UserName       string
	Password       string
	ExchangeName   string
}

func (server *Server) GetAddress() string {
	return server.serverAddress
}

func NewMessageServer(configuration *Configuration, logger core.Logger) (*Server, error) {

	if configuration == nil {
		return nil, nil
	}

	serverAddress := fmt.Sprintf("%s:%d", configuration.ServerHostName, configuration.ServerPort)

	messageServerUrl := fmt.Sprintf("amqp://%s:%s@%s/", configuration.UserName, configuration.Password, serverAddress)

	result := &Server{serverAddress: serverAddress,
		exchangeName:     configuration.ExchangeName,
		messageServerUrl: messageServerUrl,
		logger:           logger}

	return result, nil
}

type messageServerConnectData struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

func (server *Server) connectMessageQueueServer(result chan<- *messageServerConnectData) {

	for {
		conn, error := amqp.Dial(server.messageServerUrl)

		if error != nil {
			<-time.After(5 * time.Second)
			continue
		}

		ch, error := conn.Channel()

		if error != nil {
			conn.Close()
			<-time.After(5 * time.Second)
			continue
		}

		error = ch.ExchangeDeclare(
			server.exchangeName, // name
			"fanout",            // type
			false,               // durable
			false,               // auto-deleted
			false,               // internal
			false,               // no-wait
			nil,                 // arguments
		)

		if error != nil {
			ch.Close()
			conn.Close()
			<-time.After(5 * time.Second)
			continue
		}

		result <- &messageServerConnectData{channel: ch, connection: conn}
		break
	}

}

func (server *Server) sendDataToRabbitMqServer(packageChan <-chan *core.DataPackage) {
	var connectData *messageServerConnectData = nil

	connectResult := make(chan *messageServerConnectData, 1)
	defer close(connectResult)

	go server.connectMessageQueueServer(connectResult)

	for {
		select {
		case connectData = <-connectResult:

		case dataPackage := <-packageChan:
			if connectData == nil {
				break
			}

			// Посылаем пакет серверу сообщений Rabbit MQ
			body := dataPackage.Bytes()
			err := connectData.channel.Publish(
				server.exchangeName, // exchange
				"",                  // routing key
				false,               // mandatory
				false,               // immediate
				amqp.Publishing{
					//ContentType: "text/plain",
					Body: body,
				})
			if err != nil {
				connectData = nil
				go server.connectMessageQueueServer(connectResult)
			}
		}
	}
}

func (server *Server) receiveDataFromRabbitMqServer(packageChan chan<- *core.DataPackage) {
	var connectData *messageServerConnectData = nil

	connectResult := make(chan *messageServerConnectData, 1)
	defer close(connectResult)

	go server.connectMessageQueueServer(connectResult)

	select {
	case connectData = <-connectResult:
	}

	queue, _ := connectData.channel.QueueDeclare("", false, true, true, false, nil)

	queueName := queue.Name
	connectData.channel.QueueBind(queueName, "", server.exchangeName, false, nil)

	for {
		messages, _ := connectData.channel.Consume(queue.Name, "", true, true, false, false, nil)

		for msg := range messages {
			var dataPackage core.DataPackage

			r := bytes.NewReader(msg.Body)
			readPackageError := dataPackage.Read(r)

			if readPackageError == nil {
				packageChan <- &dataPackage
			}
		}
	}
}

func (server *Server) RunSender(packageChan <-chan *core.DataPackage) error {
	go server.sendDataToRabbitMqServer(packageChan)
	return nil
}

func (server *Server) RunReceiver(packageChan chan<- *core.DataPackage) error {
	go server.receiveDataFromRabbitMqServer(packageChan)
	return nil
}
