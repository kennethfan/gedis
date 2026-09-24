package datastruct

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// Redis 文献真值（非实现反推）：
// Palermo 13.361389/38.115556 → score 3479099956230698
// Catania 15.087269/37.502669 → score 3479447370796909
// 两地距离 166274.1516 m（GEODIST 默认单位 m，4 位小数）。

func TestGeoEncodeKnownScores(t *testing.T) {
	s, err := GeoEncode(13.361389, 38.115556)
	require.NoError(t, err)
	require.Equal(t, float64(3479099956230698), s)

	s, err = GeoEncode(15.087269, 37.502669)
	require.NoError(t, err)
	require.Equal(t, float64(3479447370796909), s)
}

func TestGeoEncodeInvalidRange(t *testing.T) {
	_, err := GeoEncode(200, 30)
	require.Error(t, err)
	_, err = GeoEncode(10, 90)
	require.Error(t, err)
	_, err = GeoEncode(-180.1, 0)
	require.Error(t, err)
	_, err = GeoEncode(0, -85.05112879)
	require.Error(t, err)
	// 边界合法
	_, err = GeoEncode(180, 85.05112878)
	require.NoError(t, err)
	_, err = GeoEncode(-180, -85.05112878)
	require.NoError(t, err)
}

func TestGeoDecodeRoundtrip(t *testing.T) {
	lon, lat := 13.361389, 38.115556
	s, err := GeoEncode(lon, lat)
	require.NoError(t, err)
	dlon, dlat := GeoDecode(s)
	require.Less(t, math.Abs(dlon-lon), 1e-4)
	require.Less(t, math.Abs(dlat-lat), 1e-4)
}

func TestGeoDistPalermoCatania(t *testing.T) {
	// Redis GEODIST 按解码后 cell 中心计算：GEODIST sicily Palermo Catania = 166274.1516
	sp, err := GeoEncode(13.361389, 38.115556)
	require.NoError(t, err)
	sc, err := GeoEncode(15.087269, 37.502669)
	require.NoError(t, err)
	plon, plat := GeoDecode(sp)
	clon, clat := GeoDecode(sc)
	require.InDelta(t, 166274.1516, GeoDistM(plon, plat, clon, clat), 0.01)
	// 原始坐标直接算仅作 sanity（cell 中心偏移约分米级）
	require.InDelta(t, 166274.26, GeoDistM(13.361389, 38.115556, 15.087269, 37.502669), 0.01)
}

func TestGeoDistSamePoint(t *testing.T) {
	require.Equal(t, float64(0), GeoDistM(0, 0, 0, 0))
}

func TestGeoUnitFactor(t *testing.T) {
	f, ok := GeoUnitFactor("m")
	require.True(t, ok)
	require.Equal(t, 1.0, f)
	f, ok = GeoUnitFactor("KM")
	require.True(t, ok)
	require.Equal(t, 1000.0, f)
	f, ok = GeoUnitFactor("ft")
	require.True(t, ok)
	require.Equal(t, 0.3048, f)
	f, ok = GeoUnitFactor("MI")
	require.True(t, ok)
	require.Equal(t, 1609.34, f)
	_, ok = GeoUnitFactor("yard")
	require.False(t, ok)
}
