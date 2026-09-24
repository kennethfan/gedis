package commands

import (
	"strconv"
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

func openGeoSetup(t testing.TB) {
	t.Helper()
}

// Given: 空库
// When: GEOADD 两个城市 + GEODIST/GEOPOS
// Then: Palermo-Catania 距离 166274.1516m / km 换算 / 缺失返回 nil
func Test_Geo_when_AddDistPos(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterGeo(r, store)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2},
		dispatch(r, "GEOADD", "sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"))
	require.Equal(t, protocol.BulkOf("166274.1516"), dispatch(r, "GEODIST", "sicily", "Palermo", "Catania"))
	require.Equal(t, protocol.BulkOf("166.2742"), dispatch(r, "GEODIST", "sicily", "Palermo", "Catania", "km"))
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, dispatch(r, "GEODIST", "sicily", "Palermo", "Nope"))
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, dispatch(r, "GEODIST", "missing", "Palermo", "Catania"))

	pos := dispatch(r, "GEOPOS", "sicily", "Palermo", "Nope")
	require.Equal(t, protocol.KindArray, pos.Kind)
	require.Len(t, pos.Elems, 2)
	require.Equal(t, protocol.KindArray, pos.Elems[0].Kind)
	require.Len(t, pos.Elems[0].Elems, 2)
	require.Equal(t, protocol.BulkOf("13.36138933897018433"), pos.Elems[0].Elems[0])
	require.Equal(t, protocol.BulkOf("38.11555639549629859"), pos.Elems[0].Elems[1])
	require.Equal(t, protocol.KindBulkString, pos.Elems[1].Kind)
	require.Nil(t, pos.Elems[1].Bulk)
}

// Given: 空库
// When: GEOADD NX/XX/CH + 非法坐标 + 参数错误
// Then: 语义与 Redis 一致
func Test_Geo_when_AddFlags(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterGeo(r, store)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatch(r, "GEOADD", "g", "13.361389", "38.115556", "Palermo"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatch(r, "GEOADD", "g", "NX", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0},
		dispatch(r, "GEOADD", "g", "XX", "10", "10", "Roma"))
	require.Equal(t, protocol.Value{Kind: protocol.KindError, S: "ERR invalid longitude,latitude pair 200.000000,30.000000"},
		dispatch(r, "GEOADD", "g", "200", "30", "Bad"))
	require.Equal(t, protocol.KindError, dispatch(r, "GEOADD", "g", "1", "2").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "GEOADD", "g", "1", "2", "a", "b").Kind)
}

// Given: geo key
// When: ZSCORE/ZRANGE 读同一 key
// Then: 与 z: 复用打通，score 为 geohash 整数
func Test_Geo_when_ZSetInterop(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterGeo(r, store)
	RegisterZSet(r, store)
	dispatch(r, "GEOADD", "sicily", "13.361389", "38.115556", "Palermo")
	require.Equal(t, protocol.BulkOf("3479099956230698"), dispatch(r, "ZSCORE", "sicily", "Palermo"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "ZCARD", "sicily"))
}

// Given: sicily 三城
// When: GEOSEARCH BYRADIUS/BYBOX + GEORADIUS/BYMEMBER + COUNT/ANY/DESC
// Then: 与真 Redis 7.2.6 仲裁值一致
func Test_Geo_when_SearchRadius(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterGeo(r, store)
	RegisterZSet(r, store)
	dispatch(r, "GEOADD", "sicily",
		"13.361389", "38.115556", "Palermo",
		"15.087269", "37.502669", "Catania",
		"13.583333", "37.316667", "Agrigento")

	got := dispatch(r, "GEOSEARCH", "sicily", "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km", "ASC", "WITHDIST")
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		{Kind: protocol.KindArray, Elems: []protocol.Value{protocol.BulkOf("Palermo"), protocol.BulkOf("0.0000")}},
		{Kind: protocol.KindArray, Elems: []protocol.Value{protocol.BulkOf("Agrigento"), protocol.BulkOf("90.9778")}},
		{Kind: protocol.KindArray, Elems: []protocol.Value{protocol.BulkOf("Catania"), protocol.BulkOf("166.2742")}},
	}}, got)

	got = dispatch(r, "GEOSEARCH", "sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "100", "km", "ASC")
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf("Catania"),
	}}, got)

	got = dispatch(r, "GEOSEARCH", "sicily", "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km")
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf("Agrigento"), protocol.BulkOf("Palermo"), protocol.BulkOf("Catania"),
	}}, got)

	got = dispatch(r, "GEOSEARCH", "sicily", "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km", "DESC")
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf("Catania"), protocol.BulkOf("Agrigento"), protocol.BulkOf("Palermo"),
	}}, got)

	got = dispatch(r, "GEOSEARCH", "sicily", "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km", "ASC", "COUNT", "2")
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf("Palermo"), protocol.BulkOf("Agrigento"),
	}}, got)

	got = dispatch(r, "GEORADIUSBYMEMBER", "sicily", "Palermo", "200", "km")
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf("Agrigento"), protocol.BulkOf("Palermo"), protocol.BulkOf("Catania"),
	}}, got)

	got = dispatch(r, "GEORADIUS", "sicily", "15", "37", "200", "km", "ASC", "WITHDIST")
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		{Kind: protocol.KindArray, Elems: []protocol.Value{protocol.BulkOf("Catania"), protocol.BulkOf("56.4413")}},
		{Kind: protocol.KindArray, Elems: []protocol.Value{protocol.BulkOf("Agrigento"), protocol.BulkOf("130.4235")}},
		{Kind: protocol.KindArray, Elems: []protocol.Value{protocol.BulkOf("Palermo"), protocol.BulkOf("190.4424")}},
	}}, got)
}

// Given: sicily 三城
// When: GEOSEARCH BYBOX + SEARCHSTORE/STOREDIST + GEORADIUS STORE
// Then: 与真 Redis 仲裁值一致
func Test_Geo_when_SearchBoxStore(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterGeo(r, store)
	RegisterZSet(r, store)
	dispatch(r, "GEOADD", "sicily",
		"13.361389", "38.115556", "Palermo",
		"15.087269", "37.502669", "Catania",
		"13.583333", "37.316667", "Agrigento")

	got := dispatch(r, "GEOSEARCH", "sicily", "FROMLONLAT", "15", "37", "BYBOX", "400", "400", "km", "ASC",
		"WITHCOORD", "WITHDIST", "WITHHASH")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 3)
	require.Equal(t, protocol.BulkOf("Catania"), got.Elems[0].Elems[0])
	require.Equal(t, protocol.BulkOf("56.4413"), got.Elems[0].Elems[1])
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 3479447370796909}, got.Elems[0].Elems[2])
	require.Equal(t, protocol.BulkOf("15.08726745843887329"), got.Elems[0].Elems[3].Elems[0])
	require.Equal(t, protocol.BulkOf("37.50266842333162032"), got.Elems[0].Elems[3].Elems[1])
	require.Equal(t, protocol.BulkOf("Agrigento"), got.Elems[1].Elems[0])
	require.Equal(t, protocol.BulkOf("Palermo"), got.Elems[2].Elems[0])

	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 3},
		dispatch(r, "GEOSEARCHSTORE", "out", "sicily", "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km", "ASC"))
	require.Equal(t, protocol.BulkOf("3479030013248308"), dispatch(r, "ZSCORE", "out", "Agrigento"))

	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 3},
		dispatch(r, "GEORADIUS", "sicily", "15", "37", "200", "km", "STOREDIST", "out2"))
	stored := dispatch(r, "ZSCORE", "out2", "Catania")
	storedF, err := strconv.ParseFloat(string(stored.Bulk), 64)
	require.NoError(t, err)
	require.InDelta(t, 56.4412578701582, storedF, 1e-9)
}

// Given: sicily 三城
// When: 搜索中心缺失 / GEOPOS 尾零裁剪
// Then: 与真 Redis 一致（缺失 member 报错，缺失 key 空数组）
func Test_Geo_when_MissingCenterAndTrim(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterGeo(r, store)
	dispatch(r, "GEOADD", "sicily",
		"13.361389", "38.115556", "Palermo",
		"15.087269", "37.502669", "Catania",
		"13.583333", "37.316667", "Agrigento")

	require.Equal(t, protocol.Value{Kind: protocol.KindError, S: "ERR could not decode requested zset member"},
		dispatch(r, "GEOSEARCH", "sicily", "FROMMEMBER", "Nope", "BYRADIUS", "200", "km", "ASC"))
	require.Equal(t, protocol.Value{Kind: protocol.KindError, S: "ERR could not decode requested zset member"},
		dispatch(r, "GEORADIUSBYMEMBER", "sicily", "Nope", "200", "km", "ASC"))
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}},
		dispatch(r, "GEOSEARCH", "nosuch", "FROMMEMBER", "Nope", "BYRADIUS", "200", "km", "ASC"))
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}},
		dispatch(r, "GEORADIUSBYMEMBER", "nosuch", "Nope", "200", "km"))

	pos := dispatch(r, "GEOPOS", "sicily", "Agrigento")
	require.Equal(t, protocol.BulkOf("13.5833314061164856"), pos.Elems[0].Elems[0])
	require.Equal(t, protocol.BulkOf("37.31666804993816555"), pos.Elems[0].Elems[1])
}
