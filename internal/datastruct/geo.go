package datastruct

import (
	"fmt"
	"math"
	"strings"
)

// Geo 编解码对标 Redis geohash.c（52 位、STEP=26）：
// score = interleave(lat26, lon26)，偶位 lat、奇位 lon，float64 精确承载。
// GEOADD 成员即存此 score，复用 z: 前缀的 ZSet 编码。

const (
	geoLatMin = -85.05112878
	geoLatMax = 85.05112878
	geoLonMin = -180.0
	geoLonMax = 180.0
	geoStep   = 26

	// geoEarthRadius 与 Redis EARTH_RADIUS_IN_METERS 一致，保证 GEODIST 小数一致。
	geoEarthRadius = 6372797.560856
)

// GeoEncode 把经纬度编码为 geohash score；越界返回 Redis 同文错误。
func GeoEncode(lon, lat float64) (float64, error) {
	if lon < geoLonMin || lon > geoLonMax || lat < geoLatMin || lat > geoLatMax {
		return 0, fmt.Errorf("ERR invalid longitude,latitude pair %f,%f", lon, lat)
	}
	latOffset := (lat - geoLatMin) / (geoLatMax - geoLatMin)
	lonOffset := (lon - geoLonMin) / (geoLonMax - geoLonMin)
	latOffset *= 1 << geoStep
	lonOffset *= 1 << geoStep
	return float64(interleave64(uint32(latOffset), uint32(lonOffset))), nil
}

// GeoDecode 把 score 还原为经纬度（cell 中心）。运算顺序与 Redis
// geohashDecodeAreaToLongLat 逐行一致（min/max 再取中），保证末位 ulp 相同。
func GeoDecode(score float64) (lon, lat float64) {
	sep := deinterleave64(uint64(score))
	ilato := uint32(sep)
	ilono := uint32(sep >> 32)
	latMin := geoLatMin + float64(ilato)/float64(1<<geoStep)*(geoLatMax-geoLatMin)
	latMax := geoLatMin + float64(ilato+1)/float64(1<<geoStep)*(geoLatMax-geoLatMin)
	lonMin := geoLonMin + float64(ilono)/float64(1<<geoStep)*(geoLonMax-geoLonMin)
	lonMax := geoLonMin + float64(ilono+1)/float64(1<<geoStep)*(geoLonMax-geoLonMin)
	return (lonMin + lonMax) / 2, (latMin + latMax) / 2
}

// GeoDistM 返回两点间 haversine 距离（米），公式与 Redis 一致。
func GeoDistM(lon1, lat1, lon2, lat2 float64) float64 {
	lat1r := lonLatDeg2Rad(lat1)
	lon1r := lonLatDeg2Rad(lon1)
	lat2r := lonLatDeg2Rad(lat2)
	lon2r := lonLatDeg2Rad(lon2)
	u := math.Sin((lat2r - lat1r) / 2)
	v := math.Sin((lon2r - lon1r) / 2)
	return 2.0 * geoEarthRadius * math.Asin(math.Sqrt(u*u+math.Cos(lat1r)*math.Cos(lat2r)*v*v))
}

func lonLatDeg2Rad(d float64) float64 { return d * math.Pi / 180 }

// GeoUnitFactor 返回 Redis 单位换算（米→目标单位的除数），mi 取 1609.34 与 Redis 一致。
func GeoUnitFactor(unit string) (float64, bool) {
	switch strings.ToLower(unit) {
	case "m":
		return 1, true
	case "km":
		return 1000, true
	case "ft":
		return 0.3048, true
	case "mi":
		return 1609.34, true
	}
	return 0, false
}

var interleaveMasks = [5]uint64{
	0x5555555555555555, 0x3333333333333333,
	0x0F0F0F0F0F0F0F0F, 0x00FF00FF00FF00FF,
	0x0000FFFF0000FFFF,
}
var interleaveShifts = [5]uint{1, 2, 4, 8, 16}

func interleave64(xlo, ylo uint32) uint64 {
	x, y := uint64(xlo), uint64(ylo)
	for i := 4; i >= 0; i-- {
		x = (x | (x << interleaveShifts[i])) & interleaveMasks[i]
		y = (y | (y << interleaveShifts[i])) & interleaveMasks[i]
	}
	return x | (y << 1)
}

var deinterleaveMasks = [6]uint64{
	0x5555555555555555, 0x3333333333333333,
	0x0F0F0F0F0F0F0F0F, 0x00FF00FF00FF00FF,
	0x0000FFFF0000FFFF, 0x00000000FFFFFFFF,
}
var deinterleaveShifts = [6]uint{0, 1, 2, 4, 8, 16}

func deinterleave64(v uint64) uint64 {
	x, y := v, v>>1
	for i := 0; i < 6; i++ {
		x = (x | (x >> deinterleaveShifts[i])) & deinterleaveMasks[i]
		y = (y | (y >> deinterleaveShifts[i])) & deinterleaveMasks[i]
	}
	return x | (y << 32)
}
