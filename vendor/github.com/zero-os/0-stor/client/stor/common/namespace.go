package common

type Namespace struct {
	Label string
	Stats NamespaceStat
}

type NamespaceStat struct {
	NrObjects           int64
	ReadRequestPerHour  int64
	SpaceAvailable      float64
	SpaceUsed           float64
	WriteRequestPerHour int64
}
