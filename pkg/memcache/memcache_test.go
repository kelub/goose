package memcache

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/suite"
)

type MemcacheSuite struct {
	suite.Suite

	rawClient *memcache.Client
	client    MemcacheClient
	prefix    string
}

func TestMemcacheSuite(t *testing.T) {
	suite.Run(t, new(MemcacheSuite))
}

func (suite *MemcacheSuite) SetupSuite() {
	var err error

	hosts := strings.Split(os.Getenv("MEMCACHE_HOST"), ",")

	suite.prefix = "/test"
	suite.client, err = NewMemcacheClient(MemcacheOption{
		Host:   hosts,
		Prefix: suite.prefix,
	})
	if err != nil {
		panic(err)
	}

	suite.rawClient = memcache.New(hosts...)

}

func (suite *MemcacheSuite) SetupTest() {
	suite.rawClient.FlushAll()
}

func (suite *MemcacheSuite) TestGet() {
	Convey("test get unexisted key", suite.T(), func() {
		exist, value, err := suite.client.Get("/1")
		So(err, ShouldBeNil)
		So(exist, ShouldBeFalse)
		So(value, ShouldBeNil)
	})

	err := suite.rawClient.Set(&memcache.Item{
		Key:   suite.prefix + "/1",
		Value: []byte("value"),
	})
	if err != nil {
		panic(err)
	}
	Convey("test get", suite.T(), func() {
		exist, value, err := suite.client.Get("1")
		So(err, ShouldBeNil)
		So(exist, ShouldBeTrue)
		So(string(value), ShouldEqual, "value")
	})
}

func (suite *MemcacheSuite) TestSet() {
	Convey("test set", suite.T(), func() {
		err := suite.client.Set("/1", []byte("value"), 0)
		So(err, ShouldBeNil)

		exist, value, err := suite.client.Get("1")
		So(err, ShouldBeNil)
		So(exist, ShouldBeTrue)
		So(string(value), ShouldEqual, "value")
	})

	Convey("test set with ttl", suite.T(), func() {
		err := suite.client.Set("/2", []byte("value"), 1*time.Second)
		So(err, ShouldBeNil)

		exist, value, err := suite.client.Get("2")
		So(err, ShouldBeNil)
		So(exist, ShouldBeTrue)
		So(string(value), ShouldEqual, "value")

		time.Sleep(3 * time.Second)

		exist, value, err = suite.client.Get("2")
		So(err, ShouldBeNil)
		So(value, ShouldBeNil)
		So(exist, ShouldBeFalse)
	})
}

func (suite *MemcacheSuite) TestRemove() {
	Convey("test remove nonexist key", suite.T(), func() {
		removed, err := suite.client.Remove("/1")
		So(err, ShouldBeNil)
		So(removed, ShouldBeFalse)
	})

	Convey("test remove", suite.T(), func() {
		err := suite.client.Set("/2", []byte("value"), 0)
		So(err, ShouldBeNil)

		removed, err := suite.client.Remove("/2")
		So(err, ShouldBeNil)
		So(removed, ShouldBeTrue)

		exist, value, err := suite.client.Get("2")
		So(err, ShouldBeNil)
		So(exist, ShouldBeFalse)
		So(value, ShouldBeNil)
	})
}

func (suite *MemcacheSuite) TestGetMulti() {

	Convey("test get multi", suite.T(), func() {
		var (
			err error
		)

		err = suite.client.Set("/1", []byte("value"), 0)
		So(err, ShouldBeNil)

		err = suite.client.Set("/2", []byte("value"), 0)
		So(err, ShouldBeNil)

		ret, err := suite.client.GetMulti([]string{"/1", "/2", "/3"})
		So(err, ShouldBeNil)

		So(string(ret["/1"]), ShouldEqual, "value")
		So(string(ret["/2"]), ShouldEqual, "value")
		_, ok := ret["/3"]
		So(ok, ShouldBeFalse)
	})
}

func (suite *MemcacheSuite) TestLock() {

	Convey("test lock", suite.T(), func() {
		var (
			locked  bool
			err     error
			haslock bool
		)

		locked, err = suite.client.TryLockOnce("/2")
		So(err, ShouldBeNil)
		So(locked, ShouldBeTrue)

		locked, err = suite.client.TryLockOnce("/2")
		So(err, ShouldBeNil)
		So(locked, ShouldBeFalse)

		haslock, err = suite.client.Unlock("/2")
		So(err, ShouldBeNil)
		So(haslock, ShouldBeTrue)

		haslock, err = suite.client.Unlock("/2")
		So(err, ShouldBeNil)
		So(haslock, ShouldBeFalse)
	})
}

func (suite *MemcacheSuite) TestIncr() {
	Convey("test incr", suite.T(), func() {
		var (
			err      error
			exist    bool
			value    []byte
			newvalue uint64
		)

		newvalue, err = suite.client.Increment("/1", 1)
		So(err, ShouldBeNil)
		So(newvalue, ShouldEqual, 1)

		exist, value, err = suite.client.Get("1")
		So(err, ShouldBeNil)
		So(exist, ShouldBeTrue)
		So(string(value), ShouldEqual, "1")

		newvalue, err = suite.client.Increment("/1", 1)
		So(err, ShouldBeNil)
		So(newvalue, ShouldEqual, 2)

		exist, value, err = suite.client.Get("1")
		So(err, ShouldBeNil)
		So(exist, ShouldBeTrue)
		So(string(value), ShouldEqual, "2")

		newvalue, err = suite.client.Increment("/1", -2)
		So(err, ShouldBeNil)
		So(newvalue, ShouldEqual, 0)

		exist, value, err = suite.client.Get("1")
		So(err, ShouldBeNil)
		So(exist, ShouldBeTrue)
		So(string(value), ShouldEqual, "0")

		newvalue, err = suite.client.Increment("/1", -2)
		So(err, ShouldBeNil)
		So(newvalue, ShouldEqual, 0)

		exist, value, err = suite.client.Get("1")
		So(err, ShouldBeNil)
		So(exist, ShouldBeTrue)
		So(string(value), ShouldEqual, "0")
	})
}

func (suite *MemcacheSuite) TestMaxIdleConns() {
	hosts := strings.Split(os.Getenv("MEMCACHE_HOST"), ",")
	Convey("test MaxIdleConns not set", suite.T(), func() {
		cli, err := NewMemcacheClient(MemcacheOption{
			Host:   hosts,
			Prefix: suite.prefix,
		})
		if err != nil {
			panic(err)
		}
		So(cli.client.MaxIdleConns, ShouldEqual, 10)
	})

	Convey("test MaxIdleConns set", suite.T(), func() {
		cli, err := NewMemcacheClient(MemcacheOption{
			Host:         hosts,
			Prefix:       suite.prefix,
			MaxIdleConns: 100,
		})
		if err != nil {
			panic(err)
		}
		So(cli.client.MaxIdleConns, ShouldEqual, 100)
	})
}

func (suite *MemcacheSuite) TestGetWithItem() {
	Convey("test get unexisted key", suite.T(), func() {
		exist, value, flags, err := suite.client.GetWithFlags("/getwithitem")
		So(err, ShouldBeNil)
		So(exist, ShouldBeFalse)
		So(value, ShouldBeNil)
		So(flags, ShouldEqual, 0)
	})

	err := suite.rawClient.Set(&memcache.Item{
		Key:   suite.prefix + "/getwithitem",
		Value: []byte("value"),
		Flags: 111,
	})
	if err != nil {
		panic(err)
	}
	Convey("test get", suite.T(), func() {
		exist, value, flags, err := suite.client.GetWithFlags("getwithitem")
		So(err, ShouldBeNil)
		So(exist, ShouldBeTrue)
		So(string(value), ShouldEqual, "value")
		So(flags, ShouldEqual, 111)
	})
}

func (suite *MemcacheSuite) TestSetWithFlags() {
	Convey("test set", suite.T(), func() {
		err := suite.client.SetWithFlags("/setwithitem", []byte("value"), 0, 123)
		So(err, ShouldBeNil)

		exist, value, flags, err := suite.client.GetWithFlags("setwithitem")
		So(err, ShouldBeNil)
		So(exist, ShouldBeTrue)
		So(string(value), ShouldEqual, "value")
		So(flags, ShouldEqual, 123)
	})
}
