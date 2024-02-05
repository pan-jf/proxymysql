package mysqlserver

import (
	"io"
)

func ReadMysqlPacketHeader(conn io.Reader) (*MysqlPacketHeader, error) {
	header := make([]byte, 4)

	_, err := io.ReadFull(conn, header)
	if err != nil {
		return nil, err
	}

	//fmt.Printf("%+v\n", header)

	return &MysqlPacketHeader{
		Length:     ReadUint24(header[:3]),
		SequenceId: ReadByte(header[3:]),
		HeaderByte: header,
	}, nil
}

func ReadMysqlPacketByLength(conn io.Reader, dataLength int) ([]byte, error) {
	data := make([]byte, dataLength)

	_, err := io.ReadFull(conn, data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func ReadMysqlPacket(conn io.Reader) (*MysqlPacket, error) {
	header, err := ReadMysqlPacketHeader(conn)
	if err != nil {
		return nil, err
	}
	//fmt.Printf("%+v\n", header)

	data := make([]byte, header.Length)

	_, err = io.ReadFull(conn, data)
	if err != nil {
		return nil, err
	}

	res := &MysqlPacket{
		Payload: data,
	}
	res.MysqlPacketHeader = *header

	return res, nil
}
