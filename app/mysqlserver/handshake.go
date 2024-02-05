package mysqlserver

import (
	"bytes"
	"fmt"
	"io"
	"proxymysql/app/zlog"
)

const (
	errAuthPluginMethod            = "err_auth_plugin_method"
	nativePasswordAuthPluginMethod = "mysql_native_password"
)

var DefaultHandshakeCapability uint32 = CapabilityClientLongPassword |
	CapabilityClientFoundRows |
	CapabilityClientLongFlag |
	CapabilityClientConnectWithDB |
	CapabilityClientProtocol41 |
	CapabilityClientTransactions |
	CapabilityClientSecureConnection |
	CapabilityClientMultiStatements |
	CapabilityClientMultiResults |
	CapabilityClientPluginAuth |
	CapabilityClientPluginAuthLenencClientData |
	CapabilityClientDeprecateEOF |
	CapabilityClientConnAttr |
	CapabilityClientQueryAttributes

type HandshakeV10 struct {
	ProtocolVersion  uint8
	ServerVersion    string
	AuthPluginMethod string
	ConnectionId     uint32
	CharsetCollation uint8

	ServerStatus uint16

	CapabilityFlag uint32

	AuthPluginData []byte
}

func (hk *HandshakeV10) ToByte() []byte {
	data := make([]byte, 0)

	// protocol version
	data = append(data, WriteByte(protocolVersion)...)

	// server version
	data = append(data, WriteStringNull(hk.ServerVersion)...)

	// thread id
	data = append(data, WriteUint32(hk.ConnectionId)...)

	// 	auth-plugin-data-part-1	first 8 bytes of the plugin provided data (scramble)
	data = append(data, hk.AuthPluginData[:8]...)

	// filler	0x00 byte, terminating the first part of a scramble
	data = append(data, 0x00)

	// capability_flags_1	The lower 2 bytes of the Capabilities Flags
	data = append(data, WriteUint16(uint16(hk.CapabilityFlag))...)

	data = append(data, WriteByte(CharsetUtf8mb4GeneralCiId)...)

	serverStatus := ServerStatusAutocommit
	data = append(data, WriteUint16(serverStatus)...)

	// The upper 2 bytes of the Capabilities Flags
	data = append(data, WriteUint16(uint16(hk.CapabilityFlag>>16))...)

	// Length of auth plugin data.
	// Always 21 (8 + 13).
	data = append(data, WriteByte(21)...)

	// reserved. All 0s.
	fillByte := make([]byte, 10)
	data = append(data, fillByte...)

	// auth-plugin-data-part-2	Rest of the plugin provided data (scramble), $len=MAX(13, length of auth-plugin-data - 8)
	data = append(data, hk.AuthPluginData[8:]...)

	data = append(data, WriteStringNull(hk.AuthPluginMethod)...)

	return WithHeaderPacket(data, 0)
}

type HandshakeResponse struct {
	ClientFlag       uint32
	MaxPacketSize    uint32
	Charset          uint8
	Username         string
	Password         []byte
	Database         string
	AuthPluginMethod string
	ClientAttrLen    uint64
	ClientAttrs      map[string]string
	MysqlPacketHeader
}

func (hp *HandshakeResponse) ToByte() []byte {
	res := make([]byte, 0, hp.Length)

	res = append(res, WriteUint32(hp.ClientFlag)...)
	res = append(res, WriteUint32(hp.MaxPacketSize)...)

	res = append(res, WriteByte(hp.Charset)...)

	fill := make([]byte, 23)
	res = append(res, fill...)

	res = append(res, WriteStringNull(hp.Username)...)

	if hp.ClientFlag&CapabilityClientPluginAuthLenencClientData > 0 {
		res = append(res, WriteLengthEncodedString(hp.Password)...)
	} else {
		res = append(res, uint8(len(hp.Password)))
		res = append(res, hp.Password...)
	}

	if hp.ClientFlag&CapabilityClientConnectWithDB > 0 {
		res = append(res, WriteStringNull(hp.Database)...)
	}

	if hp.ClientFlag&CapabilityClientPluginAuth > 0 {
		res = append(res, WriteStringNull(hp.AuthPluginMethod)...)
	}

	if hp.ClientFlag&CapabilityClientConnAttr > 0 {
		//res = append(res, WriteLengthEncodedInt(resp.ClientAttrLen)...)
		attrByte := ClientAttrsToByte(hp.ClientAttrs)
		attrLen := len(attrByte)
		if attrLen > 0 {
			res = append(res, WriteLengthEncodedInt(uint64(attrLen))...)
			res = append(res, attrByte...)
		}
	}

	//fmt.Printf("%+v\n", buf.Bytes())

	return WithHeaderPacket(res, hp.SequenceId)
}

func ClientAttrsToByte(clientAttrs map[string]string) []byte {
	if len(clientAttrs) == 0 {
		return nil
	}

	attrByte := make([]byte, 0)

	for k, v := range clientAttrs {
		keyByte := []byte(k)
		valByte := []byte(v)

		keyEncoded := WriteLengthEncodedInt(uint64(len(keyByte)))

		valEncoded := WriteLengthEncodedInt(uint64(len(valByte)))

		attrByte = append(attrByte, keyEncoded...)
		attrByte = append(attrByte, keyByte...)

		attrByte = append(attrByte, valEncoded...)
		attrByte = append(attrByte, valByte...)
	}

	//fmt.Printf("len:%d  attrByte:%+v\n", len(attrByte), attrByte)

	return attrByte
}

func ParseClientAttrs(data []byte) (map[string]string, error) {
	length := len(data)

	buf := bytes.NewBuffer(data)

	readLength := 0

	attrs := make(map[string]string)

	for readLength < length {
		keyLen, keyPos, ok := ReadLengthEncodedInt(buf.Bytes())
		if !ok {
			return nil, fmt.Errorf("read attrs key err")
		}

		readLength += int(keyLen) + keyPos
		buf.Next(keyPos)

		key := ReadString(buf.Next(int(keyLen)))

		valLen, valPos, ok := ReadLengthEncodedInt(buf.Bytes())
		if !ok {
			return nil, fmt.Errorf("read attrs value err")
		}

		readLength += int(valLen) + keyPos
		buf.Next(valPos)

		value := ReadString(buf.Next(int(valLen)))

		attrs[key] = value

		//fmt.Printf("%s - %s [%d]\n", key, value, readLength)
		//break
	}

	//fmt.Printf("parse attrs ok: %+v\n", attrs)

	return attrs, nil
}

func ReadHandshakeV10(conn io.Reader) (*HandshakeV10, error) {
	pk, err := ReadMysqlPacket(conn)
	if err != nil {
		return nil, err
	}

	if pk.Payload[0] != protocolVersion {
		return nil, fmt.Errorf("protocol version err: %d", pk.Payload[0])
	}

	buf := bytes.NewBuffer(pk.Payload)

	res := &HandshakeV10{}

	res.ProtocolVersion = ReadByte(buf.Next(1))

	// version
	versionByte, err := buf.ReadBytes(0x00)
	if err != nil {
		return nil, err
	}

	res.ServerVersion = ReadStringNull(versionByte)

	//fmt.Println(string(versionByte), versionByte)

	// connId
	res.ConnectionId = ReadUint32(buf.Next(4))
	//fmt.Println(binary.LittleEndian.Uint32(connId))

	fullAuthPluginData := make([]byte, 21)

	authPluginData1 := buf.Next(8)

	copy(fullAuthPluginData, authPluginData1[:8])
	//fmt.Println(string(salt1), salt1)

	// filler 0x00
	buf.Next(1)

	cap1 := buf.Next(2)
	//fmt.Println("cap1", cap1)

	fullCap := make([]byte, 4)

	copy(fullCap[:2], cap1)

	charset := buf.Next(1)
	res.CharsetCollation = ReadByte(charset)

	serverStatus := buf.Next(2)
	res.ServerStatus = ReadUint16(serverStatus)

	cap2 := buf.Next(2)
	copy(fullCap[2:], cap2)

	//fmt.Printf("fullCap: %+v\n", fullCap)
	res.CapabilityFlag = ReadUint32(fullCap)

	//res.CapFlag2 = ReadUint16(cap2)

	// auth_plugin_data_len, reserved. All 0s.
	buf.Next(1 + 10)

	authPluginData2, err := buf.ReadBytes(0x00)
	if err != nil {
		return nil, err
	}

	copy(fullAuthPluginData[8:], authPluginData2)
	res.AuthPluginData = fullAuthPluginData

	authPluginMethod, err := buf.ReadBytes(0x00)
	if err != nil {
		return nil, err
	}

	res.AuthPluginMethod = ReadStringNull(authPluginMethod)

	return res, nil
}

func ReadHandshakeResponse(conn io.Reader) (*HandshakeResponse, error) {
	pk, err := ReadMysqlPacket(conn)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(pk.Payload)

	clientFlag := ReadUint32(buf.Next(4))
	if clientFlag&CapabilityClientProtocol41 == 0 {
		return nil, fmt.Errorf("only support CLIENT_PROTOCOL_41")
	}

	if clientFlag&CapabilityClientPluginAuth == 0 {
		return nil, fmt.Errorf("unsupport without ClientPluginAuth")
	}

	res := &HandshakeResponse{}
	res.MysqlPacketHeader = pk.MysqlPacketHeader

	res.ClientFlag = clientFlag
	res.MaxPacketSize = ReadUint32(buf.Next(4))

	res.Charset = ReadByte(buf.Next(1))

	buf.Next(23)

	username, err := buf.ReadBytes(0x00)
	if err != nil {
		return nil, err
	}

	res.Username = ReadStringNull(username)

	//fmt.Println(buf.Bytes())

	if clientFlag&CapabilityClientPluginAuthLenencClientData > 0 {
		length, pos, ok := ReadLengthEncodedInt(buf.Bytes())
		if !ok {
			return nil, fmt.Errorf("ReadLengthEncodedInt err")
		}

		buf.Next(pos)

		password := buf.Next(int(length))

		res.Password = password

	} else {
		length := ReadByte(buf.Next(1))
		password := buf.Next(int(length))

		res.Password = password
	}

	if clientFlag&CapabilityClientConnectWithDB > 0 {
		db, err := buf.ReadBytes(0x00)
		if err != nil {
			return nil, err
		}

		res.Database = ReadStringNull(db)
	}

	if clientFlag&CapabilityClientPluginAuth > 0 {
		authPluginMethod, err := buf.ReadBytes(0x00)
		if err != nil {
			return nil, err
		}

		res.AuthPluginMethod = ReadStringNull(authPluginMethod)
	}

	if clientFlag&CapabilityClientConnAttr > 0 {
		length, pos, ok := ReadLengthEncodedInt(buf.Bytes())
		if ok {
			res.ClientAttrLen = length
			buf.Next(pos)

			attrsByte := make([]byte, length)
			copy(attrsByte, buf.Next(int(res.ClientAttrLen)))

			attrs, err := ParseClientAttrs(attrsByte)
			if err != nil {
				zlog.Errorf("parse client attrs err: %s", err)
			} else {
				res.ClientAttrs = attrs
			}

		}

		//fmt.Printf("%+v\n", buf.Bytes())
	}

	//fmt.Printf("%+v\n", buf.Bytes())

	return res, nil
}
