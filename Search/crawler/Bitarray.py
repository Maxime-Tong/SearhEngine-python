# SJTU EE208

import math
import GeneralHashFunctions

class Bitarray:
    def __init__(self, size):
        """ Create a bit array of a specific size """
        self.size = size
        self.bitarray = bytearray(math.ceil(size / 8.))

    def set(self, n):
        """ Sets the nth element of the bitarray """

        index = int(n / 8)
        position = n % 8
        self.bitarray[index] = self.bitarray[index] | 1 << (7 - position)

    def get(self, n):
        """ Gets the nth element of the bitarray """

        index = n // 8
        position = n % 8
        return (self.bitarray[index] & (1 << (7 - position))) > 0


if __name__ == "__main__":
    
    bitarray_obj = Bitarray(32000)
    for i in range(5):
        print("Setting index %d of bitarray .." % i)
        bitarray_obj.set(i)
        print("bitarray[%d] = %d" % (i, bitarray_obj.get(i)))


class BloomFilters():
    def __init__(self,size=32000):
        # self.k=int(math.log(2)*size/n)
        self.size=size
        self.bitarr=Bitarray(size)
    
    def lookup(self,keyword):
        for func in dir(GeneralHashFunctions):
            if func.endswith('Hash'):
                r=getattr(GeneralHashFunctions, func)(keyword)%self.size
                if not self.bitarr.get(r):return False

        return True
    
    def add(self,keyword):
        for func in dir(GeneralHashFunctions):
            if func.endswith('Hash'):
                r=getattr(GeneralHashFunctions, func)(keyword)%self.size
                self.bitarr.set(r)

        