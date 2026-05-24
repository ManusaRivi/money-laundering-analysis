package network

import (
	"net"
)

/*
 * Esto abstrae una conexion TCP con metodos para enviar y recibir datos evitando short reads y writes.
 * Compartido entre Client y Gateway.
 */

type Connection struct {
	conn net.Conn
}

func NewConnection(conn net.Conn) Connection {
	return Connection{
		conn: conn,
	}
}

func (c *Connection) Send(data []byte) error {
	err := c.sendAll(data)
	return err
}

func (c *Connection) Receive(size int) ([]byte, error) {
	data, err := c.receiveAll(size)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (c *Connection) Close() error {
	return c.conn.Close()
}

func (c *Connection) sendAll(data []byte) error {
	totalSent := 0
	for totalSent < len(data) {
		n, err := c.conn.Write(data[totalSent:])
		if err != nil {
			return err
		}
		totalSent += n
	}
	return nil
}

func (c *Connection) receiveAll(bufferSize int) ([]byte, error) {
	buffer := make([]byte, bufferSize)
	totalReceived := 0
	for totalReceived < bufferSize {
		n, err := c.conn.Read(buffer[totalReceived:])
		if err != nil {
			return nil, err
		}
		totalReceived += n
	}
	return buffer[:totalReceived], nil
}
