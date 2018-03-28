import struct

RUN_MASK=0xf
ML_MASK=0xf

def lz4zfs_decompress(src,dsize):
    """
    Decompresses src, a bytearray of compressed data.
    """
    VERBOSE=0
    ip = 4
    iend, = struct.unpack(">I",src[0:4]);
    dst = bytearray()

    try:
        while (ip < iend):
            token = src[ip]
            if VERBOSE:
                print("[%02d->%02d]: token 0x%x" %(ip-4, len(dst), token));

            ip += 1
            length = (token >> 4);
            if (length == RUN_MASK) :
                s = 255;
                while ((ip < iend) and (s == 255)) :
                    s = src[ip]
                    length += s
                    ip += 1

            if VERBOSE:
                print(" + copy length 0x%x" %(length));

            dst += src[ip:ip+length]
            ip += length

            off, = struct.unpack("<H",src[ip:ip+2]);
            ip += 2
            ref = len(dst) - off;

            length = (token & ML_MASK);
            if (length == ML_MASK):
                while (ip < iend) :
                    s = src[ip]
                    ip += 1
                    length += s;
                    if (s == 255):
                        continue;
                    break;

            if VERBOSE:
                print(" + copy rep-length %d from -%d" %(length, off));

            length += 4

            # use sliding window
            for i in range(length):
                dst += dst[len(dst)-off:len(dst)-off+1]

    except Exception as e:
        dst = None

    return dst
