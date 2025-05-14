package etcd

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

type ETCDClientV3Suite struct {
	suite.Suite
	EmbeddedEtcd *embed.Etcd

	Client *ETCDV3Client
	prefix string
}

func TestETCDClientV3Suite(t *testing.T) {
	suite.Run(t, new(ETCDClientV3Suite))
}

func (suite *ETCDClientV3Suite) SetupSuite() {
	fmt.Println("=== Starting Embedded ETCD ===")
	dataDir := "default.etcd"
	if err := os.RemoveAll(dataDir); err != nil {
		suite.FailNow("Failed to clean data dir", err)
	}

	cfg := embed.NewConfig()
	cfg.Dir = dataDir
	cfg.LogLevel = "info"

	e, err := embed.StartEtcd(cfg)
	suite.Require().NoError(err)
	suite.EmbeddedEtcd = e

	select {
	case <-e.Server.ReadyNotify():
		fmt.Println("ETCD Server is ready!")
	case <-time.After(10 * time.Second):
		suite.FailNow("ETCD Server took too long to start.")
	}

	suite.Client, err = NewETCDV3Client([]string{"0.0.0.0:2379"}, 5*time.Second)
	suite.Require().NoError(err)
	suite.prefix = "/test"
	suite.Client.SetPrefix(suite.prefix)
}

func (suite *ETCDClientV3Suite) SetupTest() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := suite.Client.client.Delete(ctx, "/", clientv3.WithPrefix())
	suite.Require().NoError(err)
}

func (suite *ETCDClientV3Suite) TearDownSuite() {
	suite.Client.client.Close()
}

func (suite *ETCDClientV3Suite) TestNewETCDV3Client() {
	Convey("test new etcd client", suite.T(), func() {
		client, err := NewETCDV3Client([]string{"0.0.0.0:2379"}, 5*time.Second)
		So(err, ShouldBeNil)
		So(client, ShouldNotBeNil)
		So(client.Hosts, ShouldResemble, []string{"0.0.0.0:2379"})
		So(client.Timeout, ShouldEqual, 5*time.Second)
		So(client.client, ShouldNotBeNil)
		So(client.kv, ShouldNotBeNil)
		So(client.Prefix, ShouldBeEmpty)
	})
}

func (suite *ETCDClientV3Suite) TestGetObject() {

	Convey("test get unexisted key", suite.T(), func() {
		exist, err := suite.Client.GetObject(context.Background(), "/1", &struct{}{})
		So(err, ShouldBeNil)
		So(exist, ShouldBeFalse)
	})

	kvc := clientv3.NewKV(suite.Client.client)
	_, err := kvc.Put(context.Background(), suite.prefix+"/1/2", "{\"value\":1}")
	if err != nil {
		panic(err)
	}

	Convey("test get a directory", suite.T(), func() {
		exist, err := suite.Client.GetObject(context.Background(), "/1", &struct{}{})
		So(err, ShouldBeNil)
		So(exist, ShouldBeFalse)
	})

	Convey("test get object", suite.T(), func() {
		v := &struct {
			Value int `json:"value"`
		}{}
		exist, err := suite.Client.GetObject(context.Background(), "/1/2", v)
		So(err, ShouldBeNil)
		So(exist, ShouldBeTrue)
		So(v.Value, ShouldEqual, 1)
	})

}

func (suite *ETCDClientV3Suite) TestGet() {
	Convey("test get unexisted key", suite.T(), func() {
		exist, _, err := suite.Client.Get(context.Background(), "/1")
		So(err, ShouldBeNil)
		So(exist, ShouldBeFalse)
	})

	kvc := clientv3.NewKV(suite.Client.client)
	_, err := kvc.Put(context.Background(), suite.prefix+"/1/2", "value")
	if err != nil {
		panic(err)
	}

	Convey("test get a directory", suite.T(), func() {
		exist, _, err := suite.Client.Get(context.Background(), "/1")
		So(err, ShouldBeNil)
		So(exist, ShouldBeFalse)
	})

	Convey("test get", suite.T(), func() {
		exist, v, err := suite.Client.Get(context.Background(), "/1/2")
		So(err, ShouldBeNil)
		So(exist, ShouldBeTrue)
		So(v, ShouldEqual, "value")
	})

	Convey("test get without leading /", suite.T(), func() {
		exist, v, err := suite.Client.Get(context.Background(), "1/2")
		So(err, ShouldBeNil)
		So(exist, ShouldBeTrue)
		So(v, ShouldEqual, "value")
	})

}

func (suite *ETCDClientV3Suite) TestWatchDir() {
	ctx, cancel := context.WithCancel(context.Background())
	ch := suite.Client.WatchDir(ctx, "1")

	time.Sleep(100 * time.Millisecond)
	go func() {
		kvc := clientv3.NewKV(suite.Client.client)
		_, err := kvc.Put(context.Background(), suite.prefix+"/1/1", "1")
		if err != nil {
			panic(err)
		}
		_, err = kvc.Put(context.Background(), suite.prefix+"/1/2", "2")
		if err != nil {
			panic(err)
		}
		//cancel watch context
		time.Sleep(time.Second)
		cancel()
	}()

	cnt := 0
	Convey("test watch", suite.T(), func() {
		for resp := range ch {
			if cnt == 0 {
				So(resp.Key, ShouldEqual, "1")
				So(resp.Value, ShouldEqual, "1")
				So(resp.Action, ShouldEqual, "PUT")
				So(resp.Dir, ShouldBeFalse)
			}
			if cnt == 1 {
				So(resp.Key, ShouldEqual, "2")
				So(resp.Value, ShouldEqual, "2")
				So(resp.Action, ShouldEqual, "PUT")
				So(resp.Dir, ShouldBeFalse)
			}
			cnt = cnt + 1
		}
		So(cnt, ShouldEqual, 2)
	})
}

func (suite *ETCDClientV3Suite) TestSet() {

	err := suite.Client.Set(context.Background(), "/1/2", "value", &SetOption{
		TTL: 2 * time.Second,
	})
	suite.Require().NoError(err)

	Convey("test set then get  directory", suite.T(), func() {
		exist, _, err := suite.Client.Get(context.Background(), "/1")
		So(err, ShouldBeNil)
		So(exist, ShouldBeFalse)
		So(int64(5*time.Second)/1000000000, ShouldEqual, 5)
	})

	Convey("test set then get", suite.T(), func() {
		exist, v, err := suite.Client.Get(context.Background(), "/1/2")
		So(err, ShouldBeNil)
		So(exist, ShouldBeTrue)
		So(v, ShouldEqual, "value")
	})

	time.Sleep(5 * time.Second)

	Convey("test set expired", suite.T(), func() {
		exist, v, err := suite.Client.Get(context.Background(), "1/2")
		So(err, ShouldBeNil)
		So(exist, ShouldBeFalse)
		So(v, ShouldEqual, "")
	})

}

func (suite *ETCDClientV3Suite) TestTraverseDir() {
	for _, k := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
		err := suite.Client.Set(context.Background(), strconv.Itoa(k), strconv.Itoa(k), nil)
		if err != nil {
			panic(err)
		}
	}

	ch, _ := suite.Client.TraverseDir(context.Background(), "", TraverseOption{
		PageSize: 2,
	})
	Convey("test traverse dir", suite.T(), func() {
		index := 1
		var resp Response
		for resp = range ch {
			So(resp.Key, ShouldEqual, strconv.Itoa(index))
			So(resp.Value, ShouldEqual, strconv.Itoa(index))
			index = index + 1
		}
		So(resp.Key, ShouldEqual, "9")
	})
}
