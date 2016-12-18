@0x89d4fcde0ae482cb;

struct Header {
  version @0 :UInt64;
  highSeq @1 :UInt64;
}

enum Case {
  uppercase @0;
  propercase @1;
}

struct Payload {

   struct Wordinfo {
    # Contains stemmed word and information about the orignal word before stemming 

    # the position of the word in the text field
    wordPos @0 :UInt64;

    # the offset of the suffix from the start of the stemmed word
    # when combined with the stemmed word gets back the orignal
    # text with case preserved
    suffixOffset @1 :UInt64;

    # the actual suffix text, which can start at any point in the stemmed word     
    suffixText @2 :Text;

    # NOTE: at some point we should contain bit flags that indicate if the original string
    # was propercase, all uppercase, contains a trailing space, a trailing period,
    # a trailing period and space, etc up to 8 flags. This would mean less information would
    # need to be stored in the suffix text for most words at the cost of 1 byte per word
    # info. 
  }
  wordinfos @0 :List(Wordinfo);
}

