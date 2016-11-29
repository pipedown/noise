@0x9266127bb5310c6c;

struct Header {
  version @0 :UInt64;
  highSeq @1 :UInt64;
}

struct Payload {
  struct Wordinfo {
    stemmedOffset @0 :UInt64;
    suffixOffset @1 :UInt64;
    suffixText @2 :Text;
  }
  wordinfos @0 :List(Wordinfo);
}
