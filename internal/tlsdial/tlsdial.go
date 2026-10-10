// Package tlsdial 是出向 dial 的集中 choke 点：跟随全局 [tls].enabled
// 切明文/TLS。main 启动时 Configure 一次；关闭时行为与 net.DialTimeout
// 逐字节一致。
//
// Settings 自立于 config.TLS 之外：config 包本就 import sentinel，而
// sentinel 经本包 dial，若本包再 import config 即成环；main 负责把
// cfg.TLS 映射过来（一行）。
package tlsdial

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"sync/atomic"
	"time"
)

// Settings 是出向 dial 的 TLS 开关快照：Enabled 跟随 [tls].enabled；
// InsecureSkipVerify 为 true 时跳过服务端证书校验（自签/内网场景）。
type Settings struct {
	Enabled            bool
	InsecureSkipVerify bool
}

var snapshot atomic.Value // 存 Settings

func init() { snapshot.Store(Settings{}) }

// Configure 原子替换 TLS 快照；传零值即切回明文模式（单测复位用）。
func Configure(s Settings) { snapshot.Store(s) }

// DialTimeout 按快照拨号：关闭→net.DialTimeout 原样；启用→TLS 握手
// （ServerName 取目标 host，MinVersion TLS1.2，系统根 CA，
// InsecureSkipVerify 跟随配置）。
func DialTimeout(network, addr string, timeout time.Duration) (net.Conn, error) {
	if !snapshot.Load().(Settings).Enabled {
		return net.DialTimeout(network, addr, timeout)
	}
	return dialTLS(network, addr, timeout, nil)
}

// dialTLS 供单测注入 RootCAs；生产传 nil 即系统根。
func dialTLS(network, addr string, timeout time.Duration, rootCAs *x509.CertPool) (net.Conn, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	return tls.DialWithDialer(&net.Dialer{Timeout: timeout}, network, addr, &tls.Config{
		ServerName:         host,
		MinVersion:         tls.VersionTLS12,
		RootCAs:            rootCAs,
		InsecureSkipVerify: snapshot.Load().(Settings).InsecureSkipVerify,
	})
}
