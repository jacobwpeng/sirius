package engine

import (
	"bytes"
	"testing"
)

func checkUnitEqual(t *testing.T, lhs RankUnit, rhs RankUnit) {
	if lhs.id != rhs.id {
		t.Errorf("id not match, %d vs %d", lhs.id, rhs.id)
	}
	if lhs.key != rhs.key {
		t.Errorf("key not match, %d vs %d", lhs.key, rhs.key)
	}
	if bytes.Compare(lhs.value, rhs.value) != 0 {
		t.Errorf("value not match, %q vs %q", lhs.value, rhs.value)
	}
}

func TestGet(t *testing.T) {
	e := NewArrayRankEngine(RankEngineConfig{MaxSize: 10})
	u := RankUnit{id: 1024, key: 10, value: []byte("Soldier76")}
	u2 := RankUnit{id: 1025, key: 12, value: []byte("McCree")}
	e.Update(u)
	e.Update(u2)

	exist, pos, out := e.Get(u.id)
	if !exist {
		t.Errorf("ID %d not exist", u.id)
	}
	if pos != 1 {
		t.Errorf("Expect rank 1, got: %d", pos)
	}
	checkUnitEqual(t, u, out)

	exist, pos, out = e.Get(u2.id)
	if !exist {
		t.Errorf("ID %d not exist", u2.id)
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

	u := RankUnit{id: 1024, key: 10, value: []byte("Soldier76")}
	u2 := RankUnit{id: 1025, key: 12, value: []byte("McCree")}
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
	u := RankUnit{id: 1024, key: 10, value: []byte("Soldier76")}
	exist, out := e.Update(u)
	if exist {
		t.Errorf("Found same id: %d", u.id)
	}
	u2 := RankUnit{id: 1024, key: 12, value: []byte("McCree")}
	exist, out = e.Update(u2)
	if !exist {
		t.Errorf("ID %d not found", u2.id)
	}
	checkUnitEqual(t, u, out)
	exist, pos, out := e.Get(u2.id)
	if pos != 0 {
		t.Errorf("Expect rank 0, got: %d", pos)
	}
	checkUnitEqual(t, u2, out)
}

func TestDelete(t *testing.T) {
	e := NewArrayRankEngine(RankEngineConfig{MaxSize: 10})
	u := RankUnit{id: 1024, key: 10, value: []byte("Soldier76")}
	u2 := RankUnit{id: 1025, key: 12, value: []byte("McCree")}
	u3 := RankUnit{id: 1026, key: 14, value: []byte("Sombra")}
	e.Update(u)
	e.Update(u2)
	e.Update(u3)

	exist, pos, out := e.Delete(1000)
	if exist {
		t.Error("Expect ID 1000 not found")
	}

	exist, pos, out = e.Delete(u3.id)
	if !exist {
		t.Errorf("Expect %d exist", u3.id)
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
	u := RankUnit{id: 1024, key: 10, value: []byte("Soldier76")}
	u2 := RankUnit{id: 1025, key: 12, value: []byte("McCree")}
	u3 := RankUnit{id: 1026, key: 14, value: []byte("Sombra")}
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
	u := RankUnit{id: 1024, key: 10}
	u2 := RankUnit{id: 1025, key: 10}
	u3 := RankUnit{id: 1024, key: 20}
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
	u := RankUnit{id: 1024, key: 10, value: []byte("Soldier76")}
	u2 := RankUnit{id: 1025, key: 12, value: []byte("McCree")}
	u3 := RankUnit{id: 1026, key: 14, value: []byte("Sombra")}
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
