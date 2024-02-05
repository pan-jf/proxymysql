package mysqlserver

import (
	"encoding/binary"
	"math/rand"
)

func WithHeaderPacket(data []byte, sequenceId uint8) []byte {
	payloadLength := len(data)

	header := make([]byte, 4)

	header[0] = byte(payloadLength)
	header[1] = byte(payloadLength >> 8)
	header[2] = byte(payloadLength >> 16)
	header[3] = sequenceId // 序列号 Sequence ID

	return append(header, data...)
}

func WriteByte(value byte) []byte {
	return []byte{value}
}

func WriteUint16(value uint16) []byte {
	data := make([]byte, 2)
	binary.LittleEndian.PutUint16(data, value)
	return data
}

func WriteUint24(value uint32) []byte {
	data := make([]byte, 3)
	_ = data[2] // early bounds check to guarantee safety of writes below
	data[0] = byte(value)
	data[1] = byte(value >> 8)
	data[2] = byte(value >> 16)
	return data
}

func WriteUint32(value uint32) []byte {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, value)
	return data
}

func WriteUint64(value uint64) []byte {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, value)
	return data
}

func WriteString(value string) []byte {
	return []byte(value)
}

func WriteStringNull(value string) []byte {
	data := make([]byte, 0, len(value)+1)
	data = append(data, []byte(value)...)
	data = append(data, 0x00)
	return data
}

func ReadString(value []byte) string {
	return string(value)
}

func ReadHexString(value []byte) string {
	return string(value)
}

func ReadStringNull(value []byte) string {
	ln := len(value)
	if ln == 0 {
		return ""
	}
	// 剔除最后一位0x00
	return string(value[:ln-1])
}

func ReadByte(value []byte) uint8 {
	return value[0]
}

func ReadUint16(value []byte) uint16 {
	return binary.LittleEndian.Uint16(value)
}

func ReadUint24(value []byte) uint32 {
	_ = value[2]
	return uint32(value[0]) | uint32(value[1])<<8 | uint32(value[2])<<16
}

func ReadUint32(value []byte) uint32 {
	return binary.LittleEndian.Uint32(value)
}

func ReadUint64(value []byte) uint64 {
	return binary.LittleEndian.Uint64(value)
}

// mysql 二进制数据（长度编码）（Length Coded Binary）
// 第一个字节值	后续字节数	长度值说明
// 0-250	0	第一个字节值即为数据的真实长度
// 251	0	空数据，数据的真实长度为零
// 252	2	后续额外2个字节标识了数据的真实长度
// 253	3	后续额外3个字节标识了数据的真实长度
// 254	8	后续额外8个字节标识了数据的真实长度
func ReadLengthEncodedInt(data []byte) (dataLength uint64, pos int, ok bool) {
	if len(data) == 0 {
		return 0, 0, false
	}

	pos = 0

	switch data[pos] {
	case 0xfb:
		// 251: NULL
		return 0, 1, true

	case 0xfc:
		// 252
		// Encoded in the next 2 bytes.
		if pos+2 >= len(data) {
			return 0, 0, false
		}

		return uint64(data[pos+1]) |
			uint64(data[pos+2])<<8, pos + 3, true
	case 0xfd:
		// 253
		// Encoded in the next 3 bytes.
		if pos+3 >= len(data) {
			return 0, 0, false
		}
		return uint64(data[pos+1]) |
			uint64(data[pos+2])<<8 |
			uint64(data[pos+3])<<16, pos + 4, true
	case 0xfe:
		// 254
		// Encoded in the next 8 bytes.
		if pos+8 >= len(data) {
			return 0, 0, false
		}
		return uint64(data[pos+1]) |
			uint64(data[pos+2])<<8 |
			uint64(data[pos+3])<<16 |
			uint64(data[pos+4])<<24 |
			uint64(data[pos+5])<<32 |
			uint64(data[pos+6])<<40 |
			uint64(data[pos+7])<<48 |
			uint64(data[pos+8])<<56, pos + 9, true
	}
	return uint64(data[pos]), pos + 1, true
}

func GetLengthEncodedIntSize(value uint64) int {
	switch {
	case value < 251:
		return 1
	case value < 1<<16:
		return 3
	case value < 1<<24:
		return 4
	default:
		return 9
	}
}

func WriteLengthEncodedInt(value uint64) []byte {
	data := make([]byte, GetLengthEncodedIntSize(value))

	switch {
	case value < 251:
		data[0] = byte(value)

	case value < 1<<16:
		data[0] = 0xfc
		data[1] = byte(value)
		data[2] = byte(value >> 8)

	case value < 1<<24:
		data[0] = 0xfd
		data[1] = byte(value)
		data[2] = byte(value >> 8)
		data[3] = byte(value >> 16)

	default:
		data[0] = 0xfe
		data[1] = byte(value)
		data[2] = byte(value >> 8)
		data[3] = byte(value >> 16)
		data[4] = byte(value >> 24)
		data[5] = byte(value >> 32)
		data[6] = byte(value >> 40)
		data[7] = byte(value >> 48)
		data[8] = byte(value >> 56)
	}

	return data
}

func WriteLengthEncodedString(strByte []byte) []byte {
	strLen := len(strByte)
	encodedInt := WriteLengthEncodedInt(uint64(strLen))

	data := make([]byte, 0, strLen+len(encodedInt))
	data = append(data, encodedInt...)
	data = append(data, strByte...)
	return data
}

func GetAuthPluginData() []byte {
	minChar := 30
	maxChar := 127
	res := make([]byte, 21)

	for k := range res {
		if k == 20 {
			k = 0x00 // 认证字符串以0x00结尾
			break
		}

		res[k] = byte(rand.Intn(maxChar-minChar) + minChar)
	}

	return res
}
