package mysqlserver

import (
	"io"
	"net"
	"proxymysql/app/conf"
	"proxymysql/app/zlog"
	"sync"
	"sync/atomic"
)

var (
	connectionId  uint32
	serverVersion = "8.0.30-tz-mysql-proxy"
)

type MysqlPacketHeader struct {
	Length     uint32
	SequenceId uint8
	HeaderByte []byte
}

type MysqlPacket struct {
	MysqlPacketHeader
	Payload []byte
}

func (pd *MysqlPacket) ToByte() []byte {
	res := make([]byte, len(pd.Payload)+4)

	copy(res[:3], WriteUint24(pd.Length))
	res[3] = pd.SequenceId

	copy(res[4:], pd.Payload)

	return res
}

type ProxyConn struct {
	clientConn net.Conn
	serverConn net.Conn
	dirPath    string
}

func NewProxyConn(clientConn net.Conn, dirPath string) *ProxyConn {
	return &ProxyConn{clientConn: clientConn, dirPath: dirPath}
}

func (p *ProxyConn) getConnectionId() uint32 {
	num := atomic.AddUint32(&connectionId, 1)
	if num == 0 {
		atomic.StoreUint32(&connectionId, 1)
		return 1
	}

	return num
}

func (p *ProxyConn) Handle() error {
	serverConn, err := p.getServerConn()
	if err != nil {
		return err
	}
	p.serverConn = serverConn

	// 先等待服务端返回handshake 在进行下一步操作
	hk, err := ReadHandshakeV10(p.serverConn)
	if err != nil {
		return err
	}

	hk.ConnectionId = p.getConnectionId()
	hk.ServerVersion = serverVersion
	// 暂时去掉ssl
	hk.CapabilityFlag &^= uint32(CapabilityClientSSL)

	// 去掉压缩
	hk.CapabilityFlag &^= uint32(CapabilityClientCanUseCompress)

	_, err = p.clientConn.Write(hk.ToByte())
	if err != nil {
		return err
	}

	resp, err := ReadHandshakeResponse(p.clientConn)
	if err != nil {
		return err
	}

	respByte := resp.ToByte()

	_, err = serverConn.Write(respByte)
	if err != nil {
		return err
	}

	err = p.authSwitch(p.serverConn)
	if err != nil {
		return err
	}

	p.copyStream()

	return nil
}

func (p *ProxyConn) copyStream() {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer func() {
			p.clientConn.Close()
			p.serverConn.Close()
			wg.Done()
		}()
		_, err := io.Copy(p.clientConn, p.serverConn)
		if err != nil {
			zlog.Errorf("serverConn -> clientConn err: %s", err)
			//errMsg := err.Error()
			//if !strings.Contains(errMsg, "use of closed network connection") {
			//	fmt.Printf("serverConn -> clientConn err: %s\n", err)
			//}
		}
	}()

	go func() {
		rq := NewRecordQuery(p.clientConn.RemoteAddr().String(), p.dirPath)

		defer func() {
			p.clientConn.Close()
			p.serverConn.Close()
			rq.Close()
			wg.Done()
		}()

		_, err := io.Copy(p.serverConn, io.TeeReader(p.clientConn, rq))

		if err != nil {
			zlog.Errorf("clientConn -> serverConn err: %s", err)
			//errMsg := err.Error()
			//if !strings.Contains(errMsg, "use of closed network connection") {
			//	fmt.Printf("clientConn -> serverConn err: %s\n", errMsg)
			//}
		}

	}()

	wg.Wait()

	zlog.Debug("copy stream stop")

}

func (p *ProxyConn) authSwitch(serverConn net.Conn) error {
	var isFinish bool

	for {
		serverResult, err := ReadMysqlPacket(serverConn)
		if err != nil {
			return err
		}

		//fmt.Printf("serverResult: %+v\n", serverResult)

		if len(serverResult.Payload) > 0 && (serverResult.Payload[0] == OKPacket || serverResult.Payload[0] == ErrPacket) {
			//fmt.Println("ok ----")
			isFinish = true
			//return nil
		}

		_, err = p.clientConn.Write(serverResult.ToByte())
		if err != nil {
			return err
		}

		if isFinish {
			return nil
		}

		clientResult, err := ReadMysqlPacket(p.clientConn)
		if err != nil {
			return err
		}

		_, err = serverConn.Write(clientResult.ToByte())
		if err != nil {
			return err
		}
	}

}

func (p *ProxyConn) getServerConn() (net.Conn, error) {
	return net.Dial("tcp", conf.App.RemoteDb)
}
