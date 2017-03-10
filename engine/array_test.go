package engine

import (
	"bytes"
	"testing"
)

func checkUnitEqual(t *testing.T, lhs RankUnit, rhs RankUnit) {
	if lhs.ID != rhs.ID {
		t.Errorf("ID not match, %d vs %d", lhs.ID, rhs.ID)
	}
	if lhs.Key != rhs.Key {
		t.Errorf("Key not match, %d vs %d", lhs.Key, rhs.Key)
	}
	if bytes.Compare(lhs.Value, rhs.Value) != 0 {
		t.Errorf("Value not match, %q vs %q", lhs.Value, rhs.Value)
	}
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
	checkUnitEqual(t, u, out)

	exist, pos, out = e.Get(u2.ID)
	if !exist {
		t.Errorf("ID %d not exist", u2.ID)
	}
	if pos != 0 {
		t.Errorf("Expect rank 0, got: %d", pos)
	}
	checkUnitEqual(t, u2, out)
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
	checkUnitEqual(t, u2, out)

	exist, out = e.GetByRank(1)
	if !exist {
		t.Error("Rank 1 not found")
	}
	checkUnitEqual(t, u, out)

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
	checkUnitEqual(t, u, out)
	exist, pos, out := e.Get(u2.ID)
	if pos != 0 {
		t.Errorf("Expect rank 0, got: %d", pos)
	}
	checkUnitEqual(t, u2, out)
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
	checkUnitEqual(t, u3, out)
	if e.Size() != 2 {
		t.Errorf("Expect size 2, got: %d", e.Size())
	}

	_, out = e.GetByRank(0)
	checkUnitEqual(t, out, u2)

	_, out = e.GetByRank(1)
	checkUnitEqual(t, out, u)
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
	checkUnitEqual(t, units[0], u3)

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
	checkUnitEqual(t, u3, out)

	_, out = e.GetByRank(1)
	checkUnitEqual(t, u2, out)
}
