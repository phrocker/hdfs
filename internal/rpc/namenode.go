package rpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	hadoop "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_common"
	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	krb "github.com/jcmturner/gokrb5/v8/client"
	"google.golang.org/protobuf/proto"
)

const (
	rpcVersion            byte = 0x09
	serviceClass          byte = 0x0
	noneAuthProtocol      byte = 0x0
	saslAuthProtocol      byte = 0xdf
	protocolClass              = "org.apache.hadoop.hdfs.protocol.ClientProtocol"
	protocolClassVersion       = 1
	handshakeCallID            = -3
	standbyExceptionClass      = "org.apache.hadoop.ipc.StandbyException"
)

const (
	backoffDuration    = 5 * time.Second
	leaseRenewInterval = 1 * time.Second
)

// NamenodeConnection represents an open connection to a namenode.
type NamenodeConnection struct {
	ClientID   []byte
	ClientName string
	User       string

	currentRequestID int32

	kerberosClient               *krb.Client
	kerberosServicePrincipleName string
	kerberosRealm                string

	dialFunc  func(ctx context.Context, network, addr string) (net.Conn, error)
	conn      net.Conn
	host      *namenodeHost
	hostList  []*namenodeHost
	transport transport

	reqLock chan struct{}
	done    chan struct{}
}

// NamenodeConnectionOptions represents the configurable options available
// for a NamenodeConnection.
type NamenodeConnectionOptions struct {
	// Addresses specifies the namenode(s) to connect to.
	Addresses []string
	// User specifies which HDFS user the client will act as. It is required
	// unless kerberos authentication is enabled, in which case it is overridden
	// by the username set in KerberosClient.
	User string
	// DialFunc is used to connect to the namenodes. If nil, then
	// (&net.Dialer{}).DialContext is used.
	DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)
	// KerberosClient is used to connect to kerberized HDFS clusters. If provided,
	// the NamenodeConnection will always mutually athenticate when connecting
	// to the namenode(s).
	KerberosClient *krb.Client
	// KerberosServicePrincipleName specifiesthe Service Principle Name
	// (<SERVICE>/<FQDN>) for the namenode(s). Like in the
	// dfs.namenode.kerberos.principal property of core-site.xml, the special
	// string '_HOST' can be substituted for the hostname in a multi-namenode
	// setup (for example: 'nn/_HOST@EXAMPLE.COM'). It is required if
	// KerberosClient is provided.
	KerberosServicePrincipleName string
}

type namenodeHost struct {
	address     string
	lastError   error
	lastErrorAt time.Time
}

// NewNamenodeConnectionWithOptions creates a new connection to a namenode with
// the given options and performs an initial handshake.
func NewNamenodeConnection(options NamenodeConnectionOptions) (*NamenodeConnection, error) {
	// Build the list of hosts to be used for failover.
	hostList := make([]*namenodeHost, len(options.Addresses))
	for i, addr := range options.Addresses {
		hostList[i] = &namenodeHost{address: addr}
	}

	var user, realm string
	user = options.User
	if options.KerberosClient != nil {
		creds := options.KerberosClient.Credentials
		user = creds.UserName()
		realm = creds.Realm()
	} else if user == "" {
		return nil, errors.New("user not specified")
	}

	// The ClientID is reused here both in the RPC headers (which requires a
	// "globally unique" ID) and as the "client name" in various requests.
	clientId := newClientID()
	c := &NamenodeConnection{
		ClientID:   clientId,
		ClientName: "go-hdfs-" + string(clientId),
		User:       user,

		kerberosClient:               options.KerberosClient,
		kerberosServicePrincipleName: options.KerberosServicePrincipleName,
		kerberosRealm:                realm,

		dialFunc:  options.DialFunc,
		hostList:  hostList,
		transport: &basicTransport{clientID: clientId},

		reqLock: make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	c.reqLock <- struct{}{}

	err := c.resolveConnection(context.Background())
	if err != nil {
		return nil, err
	}

	// Periodically renew any file leases.
	go c.renewLeases()

	return c, nil
}

func (c *NamenodeConnection) resolveConnection(ctx context.Context) error {
	if c.conn != nil {
		return nil
	}

	var err error
	if c.host != nil {
		err = c.host.lastError
	}

	for _, host := range c.hostList {
		if time.Since(host.lastErrorAt) < backoffDuration {
			continue
		}

		if c.dialFunc == nil {
			c.dialFunc = (&net.Dialer{}).DialContext
		}

		c.host = host
		c.conn, err = c.dialFunc(ctx, "tcp", host.address)
		if err != nil {
			c.markFailure(err)
			continue
		}
		if deadline, ok := ctx.Deadline(); ok {
			if err = c.conn.SetDeadline(deadline); err != nil {
				c.markFailure(err)
				continue
			}
		}

		err = c.doNamenodeHandshake()
		if err != nil {
			c.markFailure(err)
			continue
		}

		break
	}

	if c.conn == nil {
		return fmt.Errorf("no available namenodes: %s", err)
	}

	return nil
}

func (c *NamenodeConnection) markFailure(err error) {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.host.lastError = err
	c.host.lastErrorAt = time.Now()
}

// Execute performs an rpc call. It does this by sending req over the wire and
// unmarshaling the result into resp.
func (c *NamenodeConnection) Execute(method string, req proto.Message, resp proto.Message) error {
	return c.ExecuteContext(context.Background(), method, req, resp)
}

// ExecuteContext performs an RPC call bounded by ctx. The context deadline is
// applied before namenode dial and handshake, and while writing and reading the
// RPC. Waiting for another in-flight RPC is also cancelable.
func (c *NamenodeConnection) ExecuteContext(ctx context.Context, method string, req proto.Message, resp proto.Message) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.reqLock:
	}
	defer func() { c.reqLock <- struct{}{} }()
	defer func() {
		if c.conn != nil {
			_ = c.conn.SetDeadline(time.Time{})
		}
	}()

	c.currentRequestID++
	requestID := c.currentRequestID

	for {
		err := c.resolveConnection(ctx)
		if err != nil {
			return contextResult(ctx, err)
		}
		conn := c.conn
		if deadline, ok := ctx.Deadline(); ok {
			if err = conn.SetDeadline(deadline); err != nil {
				c.markFailure(err)
				return err
			}
		} else if err = conn.SetDeadline(time.Time{}); err != nil {
			c.markFailure(err)
			return err
		}
		stopInterrupt := context.AfterFunc(ctx, func() {
			_ = conn.SetDeadline(time.Now())
		})

		err = c.transport.writeRequest(conn, method, requestID, req)
		if err != nil {
			stopInterrupt()
			c.markFailure(err)
			if result := contextResult(ctx, nil); result != nil {
				return result
			}
			continue
		}

		err = c.transport.readResponse(conn, method, requestID, resp)
		stopInterrupt()
		if err != nil {
			if ctxErr := contextResult(ctx, nil); ctxErr != nil {
				c.markFailure(err)
				return ctxErr
			}
			// Only retry on a standby exception.
			if nerr, ok := err.(*NamenodeError); ok && nerr.exception == standbyExceptionClass {
				c.markFailure(err)
				continue
			}

			return err
		}

		break
	}
	return nil
}

func contextResult(ctx context.Context, fallback error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		<-ctx.Done()
		return ctx.Err()
	}
	return fallback
}

// A handshake packet:
// +-----------------------------------------------------------+
// |  Header, 4 bytes ("hrpc")                                 |
// +-----------------------------------------------------------+
// |  Version, 1 byte (default verion 0x09)                    |
// +-----------------------------------------------------------+
// |  RPC service class, 1 byte (0x00)                         |
// +-----------------------------------------------------------+
// |  Auth protocol, 1 byte (Auth method None = 0x00)          |
// +-----------------------------------------------------------+
//
//	If the auth protocol is something other than 'none', the authentication
//	handshake happens here. Otherwise, everything can be sent as one packet.
//
// +-----------------------------------------------------------+
// |  uint32 length of the next two parts                      |
// +-----------------------------------------------------------+
// |  varint length + RpcRequestHeaderProto                    |
// +-----------------------------------------------------------+
// |  varint length + IpcConnectionContextProto                |
// +-----------------------------------------------------------+
func (c *NamenodeConnection) doNamenodeHandshake() error {
	authProtocol := noneAuthProtocol
	kerberos := false
	if c.kerberosClient != nil {
		authProtocol = saslAuthProtocol
		kerberos = true
	}

	rpcHeader := []byte{
		0x68, 0x72, 0x70, 0x63, // "hrpc"
		rpcVersion, serviceClass, authProtocol,
	}

	_, err := c.conn.Write(rpcHeader)
	if err != nil {
		return err
	}

	if kerberos {
		err = c.doKerberosHandshake()
		if err != nil {
			return fmt.Errorf("SASL handshake: %s", err)
		}
	}

	rrh := newRPCRequestHeader(handshakeCallID, c.ClientID)
	cc := newConnectionContext(c.User, c.kerberosRealm)
	packet, err := makeRPCPacket(rrh, cc)
	if err != nil {
		return err
	}

	_, err = c.conn.Write(packet)
	return err
}

// renewLeases periodically renews all leases for the connection.
func (c *NamenodeConnection) renewLeases() {
	ticker := time.NewTicker(leaseRenewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			req := &hdfs.RenewLeaseRequestProto{ClientName: proto.String(c.ClientName)}
			resp := &hdfs.RenewLeaseResponseProto{}

			// Ignore any errors.
			c.Execute("renewLease", req, resp)
		case <-c.done:
			return
		}
	}
}

// Close terminates all underlying socket connections to remote server.
func (c *NamenodeConnection) Close() error {
	close(c.done)

	// Ensure that we're not concurrently renewing leases.
	<-c.reqLock
	defer func() { c.reqLock <- struct{}{} }()

	if c.conn != nil {
		return c.conn.Close()
	}

	return nil
}

func newRPCRequestHeader(id int32, clientID []byte) *hadoop.RpcRequestHeaderProto {
	return &hadoop.RpcRequestHeaderProto{
		RpcKind:  hadoop.RpcKindProto_RPC_PROTOCOL_BUFFER.Enum(),
		RpcOp:    hadoop.RpcRequestHeaderProto_RPC_FINAL_PACKET.Enum(),
		CallId:   proto.Int32(id),
		ClientId: clientID,
	}
}

func newRequestHeader(methodName string) *hadoop.RequestHeaderProto {
	return &hadoop.RequestHeaderProto{
		MethodName:                 proto.String(methodName),
		DeclaringClassProtocolName: proto.String(protocolClass),
		ClientProtocolVersion:      proto.Uint64(uint64(protocolClassVersion)),
	}
}

func newConnectionContext(user, kerberosRealm string) *hadoop.IpcConnectionContextProto {
	if kerberosRealm != "" {
		user = user + "@" + kerberosRealm
	}

	return &hadoop.IpcConnectionContextProto{
		UserInfo: &hadoop.UserInformationProto{
			EffectiveUser: proto.String(user),
		},
		Protocol: proto.String(protocolClass),
	}
}
