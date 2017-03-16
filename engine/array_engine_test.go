package engine

import (
	"bytes"
	"fmt"
	"testing"
)

func checkUnitEqual(lhs RankUnit, rhs RankUnit) error {
	if lhs.ID != rhs.ID {
		return fmt.Errorf("ID not match, %d vs %d", lhs.ID, rhs.ID)
	}
	if lhs.Key != rhs.Key {
		return fmt.Errorf("Key not match, %d vs %d", lhs.Key, rhs.Key)
	}
	if bytes.Compare(lhs.Value, rhs.Value) != 0 {
		return fmt.Errorf("Value not match, %q vs %q", lhs.Value, rhs.Value)
	}
	return nil
}

func TestGet(t *testing.T) {
	e := NewArrayRankEngine(RankEngineConfig{MaxSize: 10})
	u := RankUnit{ID: 1024, Key: 10, Value: []byte("Soldier76")}
	u2 := RankUnit{ID: 1025, Key: 12, Value: []byte("McCree")}
	e.Update(u)
	e.Update(u2)

	exist, pos, out := e.Get(u.ID)
	if !exist {
		t.Errorf("ID %d not exist", u.ID)
	}
	if pos != 1 {
		t.Errorf("Expect rank 1, got: %d", pos)
	}
	if err := checkUnitEqual(u, out); err != nil {
		t.Error(err)
	}

	exist, pos, out = e.Get(u2.ID)
	if !exist {
		t.Errorf("ID %d not exist", u2.ID)
	}
	if pos != 0 {
		t.Errorf("Expect rank 0, got: %d", pos)
	}
	if err := checkUnitEqual(u2, out); err != nil {
		t.Error(err)
	}
}

func TestGetByRank(t *testing.T) {
	e := NewArrayRankEngine(RankEngineConfig{MaxSize: 10})
	exist, out := e.GetByRank(0)
	if exist {
		t.Error("Found unit in engine")
	}

	u := RankUnit{ID: 1024, Key: 10, Value: []byte("Soldier76")}
	u2 := RankUnit{ID: 1025, Key: 12, Value: []byte("McCree")}
	e.Update(u)
	e.Update(u2)
	exist, out = e.GetByRank(0)
	if !exist {
		t.Error("Rank 0 not found")
	}
	if err := checkUnitEqual(u2, out); err != nil {
		t.Error(err)
	}

	exist, out = e.GetByRank(1)
	if !exist {
		t.Error("Rank 1 not found")
	}
	if err := checkUnitEqual(u, out); err != nil {
		t.Error(err)
	}

	exist, out = e.GetByRank(3)
	if exist {
		t.Error("Rank 3 found")
	}

	exist, out = e.GetByRank(e.Size() + 10)
	if exist {
		t.Error("Rank 3 found")
	}
}

func TestUpdate(t *testing.T) {
	e := NewArrayRankEngine(RankEngineConfig{MaxSize: 10})
	u := RankUnit{ID: 1024, Key: 10, Value: []byte("Soldier76")}
	exist, out := e.Update(u)
	if exist {
		t.Errorf("Found same ID: %d", u.ID)
	}
	u2 := RankUnit{ID: 1024, Key: 12, Value: []byte("McCree")}
	exist, out = e.Update(u2)
	if !exist {
		t.Errorf("ID %d not found", u2.ID)
	}
	if err := checkUnitEqual(u, out); err != nil {
		t.Error(err)
	}
	exist, pos, out := e.Get(u2.ID)
	if pos != 0 {
		t.Errorf("Expect rank 0, got: %d", pos)
	}
	if err := checkUnitEqual(u2, out); err != nil {
		t.Error(err)
	}
}

func TestDelete(t *testing.T) {
	e := NewArrayRankEngine(RankEngineConfig{MaxSize: 10})
	u := RankUnit{ID: 1024, Key: 10, Value: []byte("Soldier76")}
	u2 := RankUnit{ID: 1025, Key: 12, Value: []byte("McCree")}
	u3 := RankUnit{ID: 1026, Key: 14, Value: []byte("Sombra")}
	e.Update(u)
	e.Update(u2)
	e.Update(u3)

	exist, pos, out := e.Delete(1000)
	if exist {
		t.Error("Expect ID 1000 not found")
	}

	exist, pos, out = e.Delete(u3.ID)
	if !exist {
		t.Errorf("Expect %d exist", u3.ID)
	}
	if pos != 0 {
		t.Errorf("Expect rank 0, got: %d", pos)
	}
	if err := checkUnitEqual(u3, out); err != nil {
		t.Error(err)
	}
	if e.Size() != 2 {
		t.Errorf("Expect size 2, got: %d", e.Size())
	}

	_, out = e.GetByRank(0)
	if err := checkUnitEqual(out, u2); err != nil {
		t.Error(err)
	}

	_, out = e.GetByRank(1)
	if err := checkUnitEqual(out, u); err != nil {
		t.Error(err)
	}
}

func TestGetRange(t *testing.T) {
	e := NewArrayRankEngine(RankEngineConfig{MaxSize: 10})
	u := RankUnit{ID: 1024, Key: 10, Value: []byte("Soldier76")}
	u2 := RankUnit{ID: 1025, Key: 12, Value: []byte("McCree")}
	u3 := RankUnit{ID: 1026, Key: 14, Value: []byte("Sombra")}
	e.Update(u)
	e.Update(u2)
	e.Update(u3)

	units := e.GetRange(0, 1)
	if len(units) != 1 {
		t.Fatalf("Expect 1 units, got: %d", len(units))
	}
	if err := checkUnitEqual(units[0], u3); err != nil {
		t.Error(err)
	}

	units = e.GetRange(0, e.Size())
	if len(units) != int(e.Size()) {
		t.Error("Expect %d units, got: %d", e.Size(), len(units))
	}

	units = e.GetRange(e.Size()+1, 1)
	if units != nil {
		t.Error("Expect nil result")
	}

	units = e.GetRange(0, e.Size()+100)
	if len(units) != int(e.Size()) {
		t.Error("Expect %d units, got: %d", e.Size(), len(units))
	}
}

func TestSize(t *testing.T) {
	e := NewArrayRankEngine(RankEngineConfig{MaxSize: 10})
	u := RankUnit{ID: 1024, Key: 10}
	u2 := RankUnit{ID: 1025, Key: 10}
	u3 := RankUnit{ID: 1024, Key: 20}
	e.Update(u)
	e.Update(u2)
	if e.Size() != 2 {
		t.Error("Expect 2, got ", e.Size())
	}
	e.Update(u3)
	if e.Size() != 2 {
		t.Error("Expect 2, got ", e.Size())
	}
}

func TestMaxSize(t *testing.T) {
	e := NewArrayRankEngine(RankEngineConfig{MaxSize: 2})
	u := RankUnit{ID: 1024, Key: 10, Value: []byte("Soldier76")}
	u2 := RankUnit{ID: 1025, Key: 12, Value: []byte("McCree")}
	u3 := RankUnit{ID: 1026, Key: 14, Value: []byte("Sombra")}
	e.Update(u)
	e.Update(u2)
	e.Update(u3)

	if e.Size() != 2 {
		t.Errorf("Expect size 2, got: %d", e.Size())
	}
	_, out := e.GetByRank(0)
	if err := checkUnitEqual(u3, out); err != nil {
		t.Error(err)
	}

	_, out = e.GetByRank(1)
	if err := checkUnitEqual(u2, out); err != nil {
		t.Error(err)
	}
}

func TestSnapshot(t *testing.T) {
	e := NewArrayRankEngine(RankEngineConfig{MaxSize: 2})
	u := RankUnit{ID: 1024, Key: 10, Value: []byte("Soldier76")}
	u2 := RankUnit{ID: 1025, Key: 12, Value: []byte("McCree")}
	u3 := RankUnit{ID: 1026, Key: 14, Value: []byte("Sombra")}
	u4 := RankUnit{ID: 1025, Key: 12, Value: []byte("D.Va")}

	snapshot := e.CreateSnapshot()
	s1, _ := snapshot.(*ArrayRankEngine)
	if s1.Size() != 0 {
		t.Error("Expect empty size, got %d", s1.Size())
	}
	e.Update(u)
	e.Update(u2)
	snapshot = e.CreateSnapshot()
	s2, _ := snapshot.(*ArrayRankEngine)
	if s2.Size() != 2 {
		t.Error("Expect size 2, got %d", s2.Size())
	}
	_, _, v := s2.Get(u2.ID)
	if err := checkUnitEqual(v, u2); err != nil {
		t.Error(err)
	}

	if s1.Size() != 0 {
		t.Error("Snapshot changed")
	}
	e.Update(u3)
	e.Update(u4)

	_, _, v = s2.Get(u2.ID)
	if err := checkUnitEqual(v, u2); err != nil {
		t.Error(err)
	}
	_, _, v = e.Get(u2.ID)
	if err := checkUnitEqual(v, u4); err != nil {
		t.Error(err)
	}
}
