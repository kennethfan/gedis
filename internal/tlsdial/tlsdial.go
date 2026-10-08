// Package tlsdial 是出向 dial 的集中 choke 点：跟随全局 [tls].enabled
// 切明文/TLS。main 启动时 Configure 一次；关闭时行为与 net.DialTimeout
// 逐字节一致。
package tlsdial

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"sync/atomic"
	"time"

	"github.com/kennethfan/gedis/internal/config"
)

var snapshot atomic.Value // 存 config.TLS

func init() { snapshot.Store(config.TLS{}) }

// Configure 原子替换 TLS 快照；传零值即切回明文模式（单测复位用）。
func Configure(cfg config.TLS) { snapshot.Store(cfg) }

// DialTimeout 按快照拨号：关闭→net.DialTimeout 原样；启用→TLS 握手
// （ServerName 取目标 host，MinVersion TLS1.2，系统根 CA，
// InsecureSkipVerify 跟随配置）。
func DialTimeout(network, addr string, timeout time.Duration) (net.Conn, error) {
	cfg := snapshot.Load().(config.TLS)
	if !cfg.Enabled {
		return net.DialTimeout(network, addr, timeout)
	}
	return dialTLS(network, addr, timeout, nil)
}

// dialTLS 供单测注入 RootCAs；生产传 nil 即系统根。
func dialTLS(network, addr string, timeout time.Duration, rootCAs *x509.CertPool) (net.Conn, error) {
	cfg := snapshot.Load().(config.TLS)
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	return tls.DialWithDialer(&net.Dialer{Timeout: timeout}, network, addr, &tls.Config{
		ServerName:         host,
		MinVersion:         tls.VersionTLS12,
		RootCAs:            rootCAs,
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	})
}
