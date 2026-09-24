package commands

import (
	"context"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

const (
	geoSortNone = 0
	geoSortAsc  = 1
	geoSortDesc = 2
)

func (h *geoHandler) registerSearch(r *network.Router) {
	r.Register("GEOSEARCH", h.geosearch)
	r.Register("GEOSEARCHSTORE", h.geosearchstore)
	r.Register("GEORADIUS", h.georadius)
	r.Register("GEORADIUS_RO", h.georadius)
	r.Register("GEORADIUSBYMEMBER", h.georadiusbymember)
	r.Register("GEORADIUSBYMEMBER_RO", h.georadiusbymember)
}

type geoSearchOpts struct {
	fromMember string
	fromLon    float64
	fromLat    float64
	hasCenter  bool
	byRadius   bool
	radiusM    float64
	byBox      bool
	boxWM      float64
	boxHM      float64
	sort       int
	count      int64
	countSet   bool
	any        bool
	withDist   bool
	withHash   bool
	withCoord  bool
	unitFactor float64
	store      string
	storeSet   bool
	storeDist  bool
}

type geoHit struct {
	member string
	score  float64
	lon    float64
	lat    float64
	dist   float64
}

// parseGeoSearchOpts 解析 SEARCH/STORE 通用的 FROM*/BY*/WITH*/COUNT/SORT 段。
// storeAllowed 为 false 时（纯查询）拒绝 STORE/STOREDIST。
func parseGeoSearchOpts(args []protocol.Value, from int, storeAllowed bool) (geoSearchOpts, *protocol.Value) {
	var o geoSearchOpts
	hasFrom := false
	i := from
	for ; i < len(args); i++ {
		name, ok := argString(args[i])
		if !ok {
			v := errValueStr("ERR syntax error")
			return o, &v
		}
		switch strings.ToUpper(name) {
		case "FROMMEMBER":
			i++
			if i >= len(args) {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			m, ok := argString(args[i])
			if !ok {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			o.fromMember = m
			o.hasCenter = false
			hasFrom = true
		case "FROMLONLAT":
			if i+2 >= len(args) {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			lonStr, ok1 := argString(args[i+1])
			latStr, ok2 := argString(args[i+2])
			if !ok1 || !ok2 {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			lon, err1 := strconv.ParseFloat(lonStr, 64)
			lat, err2 := strconv.ParseFloat(latStr, 64)
			if err1 != nil || err2 != nil {
				v := errValueStr("ERR value is not a valid float")
				return o, &v
			}
			if lon < -180 || lon > 180 || lat < -85.05112878 || lat > 85.05112878 {
				v := errValueStr("ERR invalid longitude,latitude pair")
				return o, &v
			}
			o.fromLon, o.fromLat = lon, lat
			o.hasCenter = true
			hasFrom = true
			i += 2
		case "BYRADIUS":
			if i+2 >= len(args) {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			rStr, ok1 := argString(args[i+1])
			uStr, ok2 := argString(args[i+2])
			if !ok1 || !ok2 {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			r, err := strconv.ParseFloat(rStr, 64)
			if err != nil {
				v := errValueStr("ERR value is not a valid float")
				return o, &v
			}
			factor, valid := datastruct.GeoUnitFactor(uStr)
			if !valid {
				v := errValueStr("ERR unsupported unit provided. please use M, KM, FT, MI")
				return o, &v
			}
			o.byRadius = true
			o.byBox = false
			o.radiusM = r * factor
			o.unitFactor = factor
			i += 2
		case "BYBOX":
			if i+3 >= len(args) {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			wStr, ok1 := argString(args[i+1])
			hStr, ok2 := argString(args[i+2])
			uStr, ok3 := argString(args[i+3])
			if !ok1 || !ok2 || !ok3 {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			w, err1 := strconv.ParseFloat(wStr, 64)
			hh, err2 := strconv.ParseFloat(hStr, 64)
			if err1 != nil || err2 != nil {
				v := errValueStr("ERR value is not a valid float")
				return o, &v
			}
			factor, valid := datastruct.GeoUnitFactor(uStr)
			if !valid {
				v := errValueStr("ERR unsupported unit provided. please use M, KM, FT, MI")
				return o, &v
			}
			o.byBox = true
			o.byRadius = false
			o.boxWM = w * factor
			o.boxHM = hh * factor
			o.unitFactor = factor
			i += 3
		case "ASC":
			o.sort = geoSortAsc
		case "DESC":
			o.sort = geoSortDesc
		case "COUNT":
			i++
			if i >= len(args) {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			nStr, ok := argString(args[i])
			if !ok {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			n, err := strconv.ParseInt(nStr, 10, 64)
			if err != nil || n < 0 {
				v := errValueStr("ERR value is not an integer or out of range")
				return o, &v
			}
			o.count = n
			o.countSet = true
			if i+1 < len(args) {
				if nx, ok := argString(args[i+1]); ok && strings.EqualFold(nx, "ANY") {
					o.any = true
					i++
				}
			}
		case "WITHCOORD":
			o.withCoord = true
		case "WITHDIST":
			o.withDist = true
		case "WITHHASH":
			o.withHash = true
		case "STOREDIST":
			if !storeAllowed {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			i++
			if i >= len(args) {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			dst, ok := argString(args[i])
			if !ok {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			o.store = dst
			o.storeSet = true
			o.storeDist = true
		default:
			v := errValueStr("ERR syntax error")
			return o, &v
		}
	}
	if !hasFrom || (!o.byRadius && !o.byBox) {
		v := errValueStr("ERR syntax error")
		return o, &v
	}
	return o, nil
}

func (h *geoHandler) runSearch(ctx context.Context, key string, o geoSearchOpts) ([]geoHit, *protocol.Value) {
	z, _, err := h.zh.readZSet(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		v := errValue(err)
		return nil, &v
	}
	lon := make(map[string]float64, len(z))
	lat := make(map[string]float64, len(z))
	for m, s := range z {
		lo, la := datastruct.GeoDecode(s)
		lon[m] = lo
		lat[m] = la
	}
	var clon, clat float64
	if o.hasCenter {
		clon, clat = o.fromLon, o.fromLat
	} else {
		lo, ok1 := lon[o.fromMember]
		la, ok2 := lat[o.fromMember]
		if !ok1 || !ok2 {
			v := errValueStr("ERR could not decode requested zset member")
			return nil, &v
		}
		clon, clat = lo, la
	}
	hits := make([]geoHit, 0, len(z))
	for _, p := range sortedZSet(z) {
		m := p.m
		d := datastruct.GeoDistM(clon, clat, lon[m], lat[m])
		if o.byRadius {
			if d > o.radiusM {
				continue
			}
		} else {
			if !geoInBox(clon, clat, lon[m], lat[m], o.boxWM, o.boxHM) {
				continue
			}
		}
		hits = append(hits, geoHit{member: m, score: z[m], lon: lon[m], lat: lat[m], dist: d})
	}
	if o.countSet && o.any {
		if int64(len(hits)) > o.count {
			hits = hits[:o.count]
		}
		return hits, nil
	}
	sortOrder := o.sort
	if sortOrder == geoSortNone && o.countSet {
		sortOrder = geoSortAsc
	}
	if sortOrder != geoSortNone {
		asc := sortOrder == geoSortAsc
		sort.Slice(hits, func(a, b int) bool {
			if hits[a].dist != hits[b].dist {
				if asc {
					return hits[a].dist < hits[b].dist
				}
				return hits[a].dist > hits[b].dist
			}
			if asc {
				return hits[a].member < hits[b].member
			}
			return hits[a].member > hits[b].member
		})
	}
	if o.countSet && int64(len(hits)) > o.count {
		hits = hits[:o.count]
	}
	return hits, nil
}

// geoInBox 用经纬度近似矩形判定（中心点纬度换算经向米/度）；内部点与 Redis 一致。
func geoInBox(clon, clat, lon, lat, wM, hM float64) bool {
	const metersPerDeg = 111195.0
	cosLat := math.Cos(clat * math.Pi / 180)
	if cosLat < 1e-9 {
		cosLat = 1e-9
	}
	dLatM := absFloat(lat-clat) * metersPerDeg
	dLonM := absFloat(lon-clon) * metersPerDeg * cosLat
	return dLatM*2 <= hM && dLonM*2 <= wM
}

func absFloat(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

func formatGeoReply(hits []geoHit, o geoSearchOpts) protocol.Value {
	out := make([]protocol.Value, 0, len(hits))
	withExtra := o.withDist || o.withHash || o.withCoord
	for _, hit := range hits {
		if !withExtra {
			out = append(out, protocol.BulkOf(hit.member))
			continue
		}
		elem := []protocol.Value{protocol.BulkOf(hit.member)}
		if o.withDist {
			elem = append(elem, protocol.BulkOf(strconv.FormatFloat(hit.dist/o.unitFactor, 'f', 4, 64)))
		}
		if o.withHash {
			elem = append(elem, protocol.Value{Kind: protocol.KindInteger, I: int64(hit.score)})
		}
		if o.withCoord {
			elem = append(elem, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
				protocol.BulkOf(formatGeoCoord(hit.lon)),
				protocol.BulkOf(formatGeoCoord(hit.lat)),
			}})
		}
		out = append(out, protocol.Value{Kind: protocol.KindArray, Elems: elem})
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

func (h *geoHandler) storeSearch(ctx context.Context, hits []geoHit, o geoSearchOpts) protocol.Value {
	if o.withDist || o.withHash || o.withCoord {
		return errValueStr("ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORDS options")
	}
	z := make(map[string]float64, len(hits))
	for _, hit := range hits {
		if o.storeDist {
			z[hit.member] = hit.dist / o.unitFactor
		} else {
			z[hit.member] = hit.score
		}
	}
	if werr := h.zh.writeZSet(ctx, o.store, z, 0); werr != nil {
		return errValue(werr)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: int64(len(hits))}
}

func (h *geoHandler) geosearch(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 6 {
		return errValueStr("ERR wrong number of arguments for 'geosearch' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	o, errReply := parseGeoSearchOpts(args, 1, false)
	if errReply != nil {
		return *errReply
	}
	hits, errReply := h.runSearch(ctx, key, o)
	if errReply != nil {
		return *errReply
	}
	return formatGeoReply(hits, o)
}

func (h *geoHandler) geosearchstore(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 7 {
		return errValueStr("ERR wrong number of arguments for 'geosearchstore' command")
	}
	dst, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	src, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	rest := args[2:]
	filtered := make([]protocol.Value, 0, len(rest)+1)
	filtered = append(filtered, args[0])
	storeDist := false
	for _, a := range rest {
		if s, ok := argString(a); ok && strings.EqualFold(s, "STOREDIST") {
			storeDist = true
			continue
		}
		filtered = append(filtered, a)
	}
	o, errReply := parseGeoSearchOpts(filtered, 1, false)
	if errReply != nil {
		return *errReply
	}
	o.store = dst
	o.storeSet = true
	o.storeDist = storeDist
	hits, errReply := h.runSearch(ctx, src, o)
	if errReply != nil {
		return *errReply
	}
	return h.storeSearch(ctx, hits, o)
}

// parseRadiusOpts 解析 GEORADIUS key lon lat radius unit 之后段。
func parseRadiusOpts(args []protocol.Value, from int) (geoSearchOpts, *protocol.Value) {
	var o geoSearchOpts
	o.hasCenter = true
	i := from
	for ; i < len(args); i++ {
		name, ok := argString(args[i])
		if !ok {
			v := errValueStr("ERR syntax error")
			return o, &v
		}
		switch strings.ToUpper(name) {
		case "WITHCOORD":
			o.withCoord = true
		case "WITHDIST":
			o.withDist = true
		case "WITHHASH":
			o.withHash = true
		case "COUNT":
			i++
			if i >= len(args) {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			nStr, ok := argString(args[i])
			if !ok {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			n, err := strconv.ParseInt(nStr, 10, 64)
			if err != nil || n < 0 {
				v := errValueStr("ERR value is not an integer or out of range")
				return o, &v
			}
			o.count = n
			o.countSet = true
			if i+1 < len(args) {
				if nx, ok := argString(args[i+1]); ok && strings.EqualFold(nx, "ANY") {
					o.any = true
					i++
				}
			}
		case "ASC":
			o.sort = geoSortAsc
		case "DESC":
			o.sort = geoSortDesc
		case "STORE":
			i++
			if i >= len(args) {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			dst, ok := argString(args[i])
			if !ok {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			o.store = dst
			o.storeSet = true
		case "STOREDIST":
			i++
			if i >= len(args) {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			dst, ok := argString(args[i])
			if !ok {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			o.store = dst
			o.storeSet = true
			o.storeDist = true
		default:
			v := errValueStr("ERR syntax error")
			return o, &v
		}
	}
	return o, nil
}

func (h *geoHandler) georadius(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 5 {
		return errValueStr("ERR wrong number of arguments for 'georadius' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	lonStr, ok1 := argString(args[1])
	latStr, ok2 := argString(args[2])
	rStr, ok3 := argString(args[3])
	uStr, ok4 := argString(args[4])
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return errValueStr("ERR syntax error")
	}
	lon, err1 := strconv.ParseFloat(lonStr, 64)
	lat, err2 := strconv.ParseFloat(latStr, 64)
	radius, err3 := strconv.ParseFloat(rStr, 64)
	if err1 != nil || err2 != nil || err3 != nil {
		return errValueStr("ERR value is not a valid float")
	}
	factor, valid := datastruct.GeoUnitFactor(uStr)
	if !valid {
		return errValueStr("ERR unsupported unit provided. please use M, KM, FT, MI")
	}
	o, errReply := parseRadiusOpts(args, 5)
	if errReply != nil {
		return *errReply
	}
	o.fromLon, o.fromLat = lon, lat
	o.byRadius = true
	o.radiusM = radius * factor
	o.unitFactor = factor
	hits, errReply := h.runSearch(ctx, key, o)
	if errReply != nil {
		return *errReply
	}
	if o.storeSet {
		return h.storeSearch(ctx, hits, o)
	}
	return formatGeoReply(hits, o)
}

func (h *geoHandler) georadiusbymember(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 4 {
		return errValueStr("ERR wrong number of arguments for 'georadiusbymember' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	member, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	rStr, ok1 := argString(args[2])
	uStr, ok2 := argString(args[3])
	if !ok1 || !ok2 {
		return errValueStr("ERR syntax error")
	}
	radius, err := strconv.ParseFloat(rStr, 64)
	if err != nil {
		return errValueStr("ERR value is not a valid float")
	}
	factor, valid := datastruct.GeoUnitFactor(uStr)
	if !valid {
		return errValueStr("ERR unsupported unit provided. please use M, KM, FT, MI")
	}
	o, errReply := parseRadiusOpts(args, 4)
	if errReply != nil {
		return *errReply
	}
	o.fromMember = member
	o.hasCenter = false
	o.byRadius = true
	o.radiusM = radius * factor
	o.unitFactor = factor
	hits, errReply := h.runSearch(ctx, key, o)
	if errReply != nil {
		return *errReply
	}
	if o.storeSet {
		return h.storeSearch(ctx, hits, o)
	}
	return formatGeoReply(hits, o)
}
