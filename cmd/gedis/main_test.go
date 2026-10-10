// Command gedis 入口的监听分支测试：TLS 开关决定返回明文还是 TLS listener。
package main

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/config"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// writeSelfSignedCert 生成测试用自签证书并落盘，返回 cert/key 路径。
func writeSelfSignedCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "gedis-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	require.NoError(t, err)
	dir := t.TempDir()
	certFile = filepath.Join(dir, "server.crt")
	keyFile = filepath.Join(dir, "server.key")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	kb, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb})
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))
	return certFile, keyFile
}

func Test_ListenMain_TLS_disabled_returns_plaintext(t *testing.T) {
	// Given — TLS 未启用
	ln, err := listenMain("127.0.0.1", 0, config.TLS{})

	// When/Then — 明文 dial 成功
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()
	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	_ = conn.Close()
}

func Test_ListenMain_TLS_enabled_serves_PING_over_TLS(t *testing.T) {
	// Given — TLS 启用 + 自签证书，真服务器在 TLS listener 上 Serve
	certFile, keyFile := writeSelfSignedCert(t)
	ln, err := listenMain("127.0.0.1", 0, config.TLS{Enabled: true, CertFile: certFile, KeyFile: keyFile})
	require.NoError(t, err)
	router := network.DefaultRouter()
	router.Register("PING", func(_ context.Context, _ []protocol.Value) protocol.Value {
		return protocol.Value{Kind: protocol.KindSimpleString, S: "PONG"}
	})
	srv := network.NewServer(router)
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()

	// When — TLS 建连并发 PING
	tlsConn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", ln.Addr().String(), &tls.Config{InsecureSkipVerify: true}) //nolint:gosec // 测试自签证书
	require.NoError(t, err)
	defer func() { _ = tlsConn.Close() }()
	_, err = tlsConn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	require.NoError(t, err)

	// Then — 收到 +PONG
	rd := bufio.NewReader(tlsConn)
	line, err := rd.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "+PONG\r\n", line)

	// And — 明文连发 PING 拿不到正常回复
	raw, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer func() { _ = raw.Close() }()
	_ = raw.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = raw.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	rawRd := bufio.NewReader(raw)
	rawLine, rawErr := rawRd.ReadString('\n')
	require.Error(t, rawErr) // 非 TLS 字节触发握手失败，连接被关
	require.NotEqual(t, "+PONG\r\n", rawLine)
}

func Test_ListenMain_TLS_enabled_missing_cert_fails(t *testing.T) {
	// Given — TLS 启用但证书缺失
	_, err := listenMain("127.0.0.1", 0, config.TLS{Enabled: true, CertFile: "/nonexistent.crt", KeyFile: "/nonexistent.key"})

	// When/Then — fail-fast
	require.Error(t, err)
}
