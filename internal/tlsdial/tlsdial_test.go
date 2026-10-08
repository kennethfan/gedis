package tlsdial

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writeSelfSignedCert 生成 CN=gedis-test、带 127.0.0.1 IP SAN 的自签证书，返回 cert/key PEM。
func writeSelfSignedCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "gedis-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	require.NoError(t, err)
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	kb, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb})
	return certPEM, keyPEM
}

// startPlainServer 起明文回显行服务：读一行回 "+PONG\r\n"。
func startPlainServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = c.Close() }()
				rd := bufio.NewReader(c)
				if _, err := rd.ReadString('\n'); err != nil {
					return
				}
				_, _ = c.Write([]byte("+PONG\r\n"))
			}()
		}
	}()
	return ln.Addr().String()
}

// startTLSServer 起 TLS 回显行服务（自签证书，服务端不校验客户端）。
func startTLSServer(t *testing.T, certPEM, keyPEM []byte) string {
	t.Helper()
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = c.Close() }()
				rd := bufio.NewReader(c)
				if _, err := rd.ReadString('\n'); err != nil {
					return
				}
				_, _ = c.Write([]byte("+PONG\r\n"))
			}()
		}
	}()
	return ln.Addr().String()
}

func pingPong(t *testing.T, conn net.Conn) {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	require.NoError(t, err)
	line, err := bufio.NewReader(conn).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "+PONG\r\n", line)
}

func Test_Tlsdial_plaintext_mode_passes_through(t *testing.T) {
	// Given — 明文模式 + 明文服务
	Configure(Settings{})
	t.Cleanup(func() { Configure(Settings{}) })
	addr := startPlainServer(t)

	// When/Then — 直通成功
	conn, err := DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	pingPong(t, conn)
}

func Test_Tlsdial_tls_insecure_skips_verify(t *testing.T) {
	// Given — TLS + insecure=true + 自签服务
	certPEM, keyPEM := writeSelfSignedCert(t)
	Configure(Settings{Enabled: true, InsecureSkipVerify: true})
	t.Cleanup(func() { Configure(Settings{}) })
	addr := startTLSServer(t, certPEM, keyPEM)

	// When/Then — 跳过校验建连成功
	conn, err := DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	pingPong(t, conn)
}

func Test_Tlsdial_tls_verify_rejects_selfsigned(t *testing.T) {
	// Given — TLS + insecure=false + 自签服务（系统根不信任）
	certPEM, keyPEM := writeSelfSignedCert(t)
	Configure(Settings{Enabled: true})
	t.Cleanup(func() { Configure(Settings{}) })
	addr := startTLSServer(t, certPEM, keyPEM)

	// When/Then — 握手失败
	_, err := DialTimeout("tcp", addr, 5*time.Second)
	require.Error(t, err)
}

func Test_Tlsdial_tls_servername_matches_ip_san(t *testing.T) {
	// Given — 自签 cert（含 127.0.0.1 IP SAN）+ 其 pool 做根
	certPEM, keyPEM := writeSelfSignedCert(t)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(certPEM))
	Configure(Settings{Enabled: true})
	t.Cleanup(func() { Configure(Settings{}) })
	addr := startTLSServer(t, certPEM, keyPEM)

	// When/Then — pool 信任后建连成功，证明 ServerName=127.0.0.1 命中 IP SAN
	conn, err := dialTLS("tcp", addr, 5*time.Second, pool)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	pingPong(t, conn)
}
