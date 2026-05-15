package lsm

// export_test.go exposes internal constructors and types to the _test package
// without polluting the public API.

// NewMemTableForTest creates a MemTable with the given max size.
func NewMemTableForTest(maxSize int64) *MemTable { return newMemTable(maxSize) }

// MemTable.Put/Get/Delete/Iterator are already exported on the type.

// NewBuilderForTest opens a Builder for testing.
func NewBuilderForTest(path string, opts Options) (*Builder, error) {
	return newBuilder(path, opts)
}

// Builder.Add and Builder.Finish are already exported on the type.

// OpenSSTableForTest opens an SSTable for testing.
func OpenSSTableForTest(path string) (*SSTable, error) { return openSSTable(path) }

// SSTable.Get, Close, Path are already exported on the type.

// NewSSTIterForTest creates an SSTable iterator for testing.
func NewSSTIterForTest(sst *SSTable) *SSTIter { return newSSTIter(sst) }

// DefaultOptions returns options with all defaults applied.
func DefaultOptions() Options {
	o := Options{}
	o.setDefaults()
	return o
}
