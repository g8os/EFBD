package lba

// SectorStorage defines the API for a persistent storage,
// used to fetch sectors from which aren't cached yet,
// and to store sectors which are evicted from a cache.
// NOTE: a sectorStorage is not guaranteed to be thread-safe!
type SectorStorage interface {
	// GetSector fetches a sector from a storage,
	// returning an error if this wasn't possible.
	GetSector(index int64) (*Sector, error)

	// SetSector stores a sector in persistent storage.
	SetSector(index int64, sector *Sector) error
}
