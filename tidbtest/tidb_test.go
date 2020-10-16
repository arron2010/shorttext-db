package tidbtest

import (
	"context"
	"fmt"
	"github.com/pingcap/check"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/testkit"

	"testing"
	"time"
)

type testDBSuite struct {
	cluster    *mocktikv.Cluster
	mvccStore  mocktikv.MVCCStore
	store      kv.Storage
	dom        *domain.Domain
	schemaName string
	tk         *testkit.TestKit
	s          session.Session
	lease      time.Duration
	autoIDStep int64
}

func setUpSuite(c *check.C) {
	var err error
	var s *testDBSuite
	s = &testDBSuite{}
	s.lease = 600 * time.Millisecond
	session.SetSchemaLease(s.lease)
	session.DisableStats4Test()
	s.schemaName = "test_db"
	s.autoIDStep = autoid.GetStep()
	ddl.SetWaitTimeWhenErrorOccurred(0)

	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(s.cluster)
	s.mvccStore = mocktikv.MustNewMVCCStore()
	s.store, err = mockstore.NewMockTikvStore(
		mockstore.WithCluster(s.cluster),
		mockstore.WithMVCCStore(s.mvccStore),
	)

	s.dom, err = session.BootstrapSession(s.store)

	s.s, err = session.CreateSession4Test(s.store)

	_, err = s.s.Execute(context.Background(), "create database test_db")

	s.s.Execute(context.Background(), "set @@global.tidb_max_delta_schema_count= 4096")

	s.tk = testkit.NewTestKit(c, s.store)
	fmt.Println(err)
}

func TestInsertSql(t *testing.T) {
	c := &check.C{}
	setUpSuite(c)

}
