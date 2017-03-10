package engine

type RankUnit struct {
	ID    uint64
	Key   uint64
	Value []byte
}

type RankEngineConfig struct {
	MaxSize uint32
}

type RankEngine interface {
	Size() uint32
	Get(id uint64) (bool, uint32, RankUnit)
	GetByRank(pos uint32) (bool, RankUnit)
	GetRange(pos, num uint32) []RankUnit
	Update(e RankUnit) (bool, RankUnit)
	Delete(id uint64) (bool, uint32, RankUnit)
}
