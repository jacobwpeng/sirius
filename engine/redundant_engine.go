package engine

type RedundantRankEngine struct {
	config     RankEngineConfig
	underlying RankEngine
}

func NewRedundantRankEngine(config RankEngineConfig) *RedundantRankEngine {
	underlyingConfig := config
	underlyingConfig.MaxSize = config.MaxSize + config.RedundantNodeNum
	return &RedundantRankEngine{
		config:     config,
		underlying: NewArrayRankEngine(underlyingConfig),
	}
}

func (e *RedundantRankEngine) Config() RankEngineConfig {
	return e.config
}

func (e *RedundantRankEngine) Size() uint32 {
	if e.underlying.Size() > e.config.MaxSize {
		return e.config.MaxSize
	}
	return e.underlying.Size()
}

func (e *RedundantRankEngine) Get(id uint64) (bool, uint32, RankUnit) {
	exist, pos, u := e.underlying.Get(id)
	if pos >= e.config.MaxSize {
		return false, 0, RankUnit{}
	}
	return exist, pos, u
}

func (e *RedundantRankEngine) GetByRank(pos uint32) (bool, RankUnit) {
	if pos >= e.config.MaxSize {
		return false, RankUnit{}
	}
	return e.underlying.GetByRank(pos)
}

func (e *RedundantRankEngine) GetRange(pos, num uint32) []RankUnit {
	if pos >= e.Size() {
		return nil
	}
	n := num
	if pos+num >= e.Size() {
		n = e.Size() - pos
	}
	return e.underlying.GetRange(pos, n)
}

func (e *RedundantRankEngine) Update(u RankUnit) (bool, RankUnit) {
	exist, pos, last := e.underlying.Get(u.ID)
	if pos >= e.config.MaxSize {
		exist = false
		last = RankUnit{}
	}
	e.underlying.Update(u)
	return exist, last
}

func (e *RedundantRankEngine) Delete(id uint64) (bool, uint32, RankUnit) {
	exist, pos, u := e.underlying.Delete(id)
	if pos >= e.config.MaxSize {
		return false, 0, RankUnit{}
	}
	return exist, pos, u
}

func (e *RedundantRankEngine) CreateSnapshot() RankEngine {
	snapshot := &RedundantRankEngine{
		config:     e.config,
		underlying: e.underlying.CreateSnapshot(),
	}
	return snapshot
}
