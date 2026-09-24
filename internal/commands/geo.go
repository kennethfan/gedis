package commands

import (
	"context"
	"strconv"
	"strings"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// RegisterGeo 注册 Geo 命令；存储复用 z: 前缀的 ZSet 读写（member 名直通，score 为 geohash）。
// 查询族（SEARCH/RADIUS/STORE）由 geo_search.go 追加。
func RegisterGeo(r *network.Router, kv KV) {
	h := &geoHandler{kv: kv, zh: &zsetHandler{kv: kv}}
	r.Register("GEOADD", h.geoadd)
	r.Register("GEODIST", h.geodist)
	r.Register("GEOPOS", h.geopos)
	h.registerSearch(r)
}

type geoHandler struct {
	kv KV
	zh *zsetHandler
}

func (h *geoHandler) geoadd(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 4 {
		return errValueStr("ERR wrong number of arguments for 'geoadd' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	nx, xx, ch := false, false, false
	i := 1
	for ; i < len(args); i++ {
		s, ok := argString(args[i])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		switch strings.ToUpper(s) {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "CH":
			ch = true
		default:
			goto triples
		}
	}
triples:
	rest := args[i:]
	if len(rest) == 0 || len(rest)%3 != 0 {
		return errValueStr("ERR syntax error")
	}
	z, expiry, err := h.zh.readZSet(ctx, key)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		z = make(map[string]float64)
	}
	var added, changed int64
	for j := 0; j < len(rest); j += 3 {
		lonStr, ok := argString(rest[j])
		if !ok {
			return errValueStr("ERR value is not a valid float")
		}
		latStr, ok := argString(rest[j+1])
		if !ok {
			return errValueStr("ERR value is not a valid float")
		}
		lon, err := strconv.ParseFloat(lonStr, 64)
		if err != nil {
			return errValueStr("ERR value is not a valid float")
		}
		lat, err := strconv.ParseFloat(latStr, 64)
		if err != nil {
			return errValueStr("ERR value is not a valid float")
		}
		m, ok := argString(rest[j+2])
		if !ok {
			return errValueStr("ERR invalid member")
		}
		score, err := datastruct.GeoEncode(lon, lat)
		if err != nil {
			return errValue(err)
		}
		cur, exists := z[m]
		if !exists {
			if xx {
				continue
			}
			z[m] = score
			added++
			changed++
			continue
		}
		if nx {
			continue
		}
		if score != cur {
			z[m] = score
			changed++
		}
	}
	if werr := h.zh.writeZSet(ctx, key, z, expiry); werr != nil {
		return errValue(werr)
	}
	if ch {
		return protocol.Value{Kind: protocol.KindInteger, I: changed}
	}
	return protocol.Value{Kind: protocol.KindInteger, I: added}
}

func (h *geoHandler) geoMembers(ctx context.Context, key string, members []string) (map[string][2]float64, *protocol.Value) {
	z, _, err := h.zh.readZSet(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		v := errValue(err)
		return nil, &v
	}
	out := make(map[string][2]float64, len(members))
	for _, m := range members {
		s, exists := z[m]
		if !exists {
			continue
		}
		lon, lat := datastruct.GeoDecode(s)
		out[m] = [2]float64{lon, lat}
	}
	return out, nil
}

func geoMemberArgs(args []protocol.Value, from int) ([]string, *protocol.Value) {
	members := make([]string, 0, len(args)-from)
	for _, a := range args[from:] {
		m, ok := argString(a)
		if !ok {
			v := errValueStr("ERR invalid member")
			return nil, &v
		}
		members = append(members, m)
	}
	return members, nil
}

func (h *geoHandler) geodist(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 3 || len(args) > 4 {
		return errValueStr("ERR wrong number of arguments for 'geodist' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	members, errReply := geoMemberArgs(args, 1)
	if errReply != nil {
		return *errReply
	}
	members = members[:2]
	unit := "m"
	if len(args) == 4 {
		u, ok := argString(args[3])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		factor, valid := datastruct.GeoUnitFactor(u)
		if !valid {
			return errValueStr("ERR unsupported unit provided. please use M, KM, FT, MI")
		}
		unit = u
		_ = factor
	}
	pts, errReply := h.geoMembers(ctx, key, members)
	if errReply != nil {
		return *errReply
	}
	if pts == nil {
		return protocol.Value{Kind: protocol.KindBulkString}
	}
	p1, ok1 := pts[members[0]]
	p2, ok2 := pts[members[1]]
	if !ok1 || !ok2 {
		return protocol.Value{Kind: protocol.KindBulkString}
	}
	factor, _ := datastruct.GeoUnitFactor(unit)
	d := datastruct.GeoDistM(p1[0], p1[1], p2[0], p2[1]) / factor
	return protocol.BulkOf(strconv.FormatFloat(d, 'f', 4, 64))
}

func (h *geoHandler) geopos(ctx context.Context, args []protocol.Value) protocol.Value {	if len(args) < 2 {
		return errValueStr("ERR wrong number of arguments for 'geopos' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	members, errReply := geoMemberArgs(args, 1)
	if errReply != nil {
		return *errReply
	}
	pts, errReply := h.geoMembers(ctx, key, members)
	if errReply != nil {
		return *errReply
	}
	out := make([]protocol.Value, 0, len(members))
	for _, m := range members {
		if pts == nil {
			out = append(out, protocol.Value{Kind: protocol.KindBulkString})
			continue
		}
		p, ok := pts[m]
		if !ok {
			out = append(out, protocol.Value{Kind: protocol.KindBulkString})
			continue
		}
		out = append(out, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
			protocol.BulkOf(formatGeoCoord(p[0])),
			protocol.BulkOf(formatGeoCoord(p[1])),
		}})
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

// formatGeoCoord 对标 GEOPOS/WITHCOORD：%.17f 去尾零（与 ZSCORE 的 shortest 规则不同）。
func formatGeoCoord(f float64) string {
	s := strconv.FormatFloat(f, 'f', 17, 64)
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	return s
}
