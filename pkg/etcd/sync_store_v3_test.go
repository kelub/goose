package etcd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

type SyncStoreSuite struct {
	suite.Suite
	EmbeddedEtcd *embed.Etcd

	Client *ETCDV3Client
}

func TestSyncStoreSuite(t *testing.T) {
	suite.Run(t, new(SyncStoreSuite))
}

func (suite *SyncStoreSuite) SetupSuite() {
	fmt.Println("=== Starting Embedded ETCD ===")
	dataDir := "default.etcd"
	if err := os.RemoveAll(dataDir); err != nil {
		suite.FailNow("Failed to clean data dir", err)
	}

	cfg := embed.NewConfig()
	cfg.Dir = dataDir
	cfg.LogLevel = "info"

	lpurl, _ := url.Parse("http://localhost:2480")
	lcurl, _ := url.Parse("http://localhost:2479")
	cfg.ListenPeerUrls = []url.URL{*lpurl}
	cfg.ListenClientUrls = []url.URL{*lcurl}

	e, err := embed.StartEtcd(cfg)
	suite.Require().NoError(err)
	suite.EmbeddedEtcd = e

	select {
	case <-e.Server.ReadyNotify():
		fmt.Println("ETCD Server is ready!")
	case <-time.After(10 * time.Second):
		suite.FailNow("ETCD Server took too long to start.")
	}

	suite.Client, err = NewETCDV3Client([]string{"0.0.0.0:2479"}, 5*time.Second)
	suite.Require().NoError(err)
}

func (suite *SyncStoreSuite) SetupTest() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := suite.Client.client.Delete(ctx, "/", clientv3.WithPrefix())
	suite.Require().NoError(err)
}

func (suite *SyncStoreSuite) TearDownSuite() {

}

func (suite *SyncStoreSuite) TestSyncStoreCallback() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, k := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
		err := suite.Client.Set(ctx, path.Join("/1", strconv.Itoa(k)), strconv.Itoa(k), nil)
		if err != nil {
			panic(err)
		}
	}

	var m map[string]string = map[string]string{}

	_, err := NewETCDSyncStoreFromClient(ctx, "/1", suite.Client, func(resp Response) {
		if resp.IsPut() {
			m[resp.Key] = resp.Value + "."
		} else if resp.IsDelete() {
			delete(m, resp.Key)
		}
	})
	if err != nil {
		panic(err)
	}

	Convey("test sync store callback", suite.T(), func() {
		So(len(m), ShouldEqual, 9)
		for _, k := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
			So(m[strconv.Itoa(k)], ShouldEqual, strconv.Itoa(k)+".")
		}
	})

	for _, k := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
		err := suite.Client.Set(ctx, path.Join("/1", strconv.Itoa(k)), strconv.Itoa(k+1), nil)
		if err != nil {
			panic(err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	Convey("test sync store callback after update", suite.T(), func() {
		So(len(m), ShouldEqual, 9)
		for _, k := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
			So(m[strconv.Itoa(k)], ShouldEqual, strconv.Itoa(k+1)+".")
		}
	})
}

func (suite *SyncStoreSuite) TestSyncStore() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, k := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
		err := suite.Client.Set(ctx, path.Join("/1", strconv.Itoa(k)), strconv.Itoa(k), nil)
		if err != nil {
			panic(err)
		}
	}

	ss, err := NewETCDSyncStoreFromClient(ctx, "/1", suite.Client)
	if err != nil {
		panic(err)
	}

	Convey("test sync store", suite.T(), func() {
		So(ss.Len(), ShouldEqual, 9)
		So(len(ss.Map()), ShouldEqual, 9)
		for _, k := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
			exists, v, err := ss.Get(strconv.Itoa(k))
			So(exists, ShouldBeTrue)
			So(v, ShouldEqual, strconv.Itoa(k))
			So(err, ShouldBeNil)
		}
	})

	for _, k := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
		err := suite.Client.Set(ctx, path.Join("/1", strconv.Itoa(k)), strconv.Itoa(k+1), nil)
		if err != nil {
			panic(err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	Convey("test sync store after update", suite.T(), func() {
		So(ss.Len(), ShouldEqual, 9)
		So(len(ss.Map()), ShouldEqual, 9)
		for _, k := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
			exists, v, err := ss.Get(strconv.Itoa(k))
			So(exists, ShouldBeTrue)
			So(v, ShouldEqual, strconv.Itoa(k+1))
			So(err, ShouldBeNil)
		}
	})
}

func (suite *SyncStoreSuite) TestSyncStoreDirWithSlash() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, k := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
		err := suite.Client.Set(ctx, path.Join("/1", strconv.Itoa(k)), strconv.Itoa(k), nil)
		if err != nil {
			panic(err)
		}
	}

	ss, err := NewETCDSyncStoreFromClient(ctx, "/1/", suite.Client)
	if err != nil {
		panic(err)
	}

	Convey("test sync store", suite.T(), func() {
		So(ss.Len(), ShouldEqual, 9)
		So(len(ss.Map()), ShouldEqual, 9)
		for _, k := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
			exists, v, err := ss.Get(strconv.Itoa(k))
			So(exists, ShouldBeTrue)
			So(v, ShouldEqual, strconv.Itoa(k))
			So(err, ShouldBeNil)
		}
	})

	for _, k := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
		err := suite.Client.Set(ctx, path.Join("/1/", strconv.Itoa(k)), strconv.Itoa(k+1), nil)
		if err != nil {
			panic(err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	Convey("test sync store after update", suite.T(), func() {
		So(ss.Len(), ShouldEqual, 9)
		So(len(ss.Map()), ShouldEqual, 9)
		for _, k := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
			exists, v, err := ss.Get(strconv.Itoa(k))
			So(exists, ShouldBeTrue)
			So(v, ShouldEqual, strconv.Itoa(k+1))
			So(err, ShouldBeNil)
		}
	})
}
