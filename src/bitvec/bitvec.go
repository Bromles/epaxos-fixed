package bitvec

type BitVec []uint64

func New(size uint32) BitVec {
	aSize := size / 64
	if aSize*64 < size {
		aSize++
	}
	bv := BitVec(make([]uint64, aSize))
	bv.Clear()
	return bv
}

func (bv BitVec) Clear() {
	for i := 0; i < len(bv); i++ {
		bv[i] = 0
	}
}

func (bv BitVec) GetBit(pos uint32) bool {
	return (bv[pos>>6] & (1 << (pos & uint32(63)))) != 0
}

func (bv BitVec) SetBit(pos uint32) {
	bv[pos>>6] |= uint64(1) << (pos & uint32(63))
}

func (bv BitVec) ResetBit(pos uint32) {
	bv[pos>>6] &= ^(uint64(1) << (pos & uint32(63)))
}
