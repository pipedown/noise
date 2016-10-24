extern crate stemmer;

use self::stemmer::Stemmer;


//#[derive(Debug)]
pub struct Stems<'a> {
    text: &'a str,
    // The current cursor position
    current: usize,
    stemmer: Stemmer,
}

#[derive(Debug, PartialEq)]
pub struct StemmedWord {
    // Where the stemmed word starts
    pub stemmed_offset: usize,
    // Where the suffix starts
    pub suffix_offset: usize,
    // The length of the suffix
    suffix_len: usize,
    pub stemmed: String,
    pub suffix: String,
}


impl<'a> Stems<'a> {
    pub fn new(text: &str) -> Stems {
        Stems{
            text: text,
            current: 0,
            stemmer: Stemmer::new("english").unwrap(),
        }
    }

    /// Return the *byte* length of the common prefix between two strings
    fn common_prefix_len(aa: &str, bb: &str) -> usize {
        let mut count = 0;
        for (charsa, charsb) in aa.chars().zip(bb.chars()) {
            if charsa != charsb {
                break;
            }
            count += charsa.len_utf8();
          }
        count
    }
}

impl<'a> Iterator for Stems<'a> {
    type Item = StemmedWord;

    fn next(&mut self) -> Option<StemmedWord> {
        let text = &self.text[self.current..];
        let mut word_start = 0;
        let mut word_len = 0;
        for cc in text.chars() {
            // A character that is part of a word we want to stem
            if cc.is_alphabetic() {
                word_len += cc.len_utf8();
            }
            else {
                // There might be characters we don't care about for stemming
                // (whitespace and numbers) before the word
                if word_len == 0 {
                    word_start += cc.len_utf8();
                }
                // We've reached the end of the word
                else  {
                    break;
                }
            }
        }
        // No word found
        if word_len == 0 {
            None
        } else {
            // Do the actual stemming
            let input = text[word_start..word_start + word_len].to_lowercase();
            // The lowercase version might contain more bytes than the uppercase one:
            // e.g. İ => i̇
            let lowercase_len = input.len();
            let stemmed = self.stemmer.stem(&input);
            let prefix_len = Stems::common_prefix_len(&stemmed, &input);
            println!("{} {} {}", input, stemmed, prefix_len);

            let ret = StemmedWord {
                stemmed_offset: self.current + word_start,
                suffix_offset: self.current + word_start + prefix_len,
                suffix_len: lowercase_len - prefix_len,
                stemmed: stemmed,
                suffix: (&input[prefix_len..lowercase_len]).to_string(),
            };
            self.current += word_start + word_len;
            Some(ret)
        }
    }

}


#[cfg(test)]
mod tests {
    use super::{StemmedWord, Stems};

    #[test]
    fn test_stems_lowercase() {
        let input = "These words deeply test smoothly that stemming";
        let stems = Stems::new(input);
        let result = Stems::new(input).collect::<Vec<StemmedWord>>();
        let expected = vec![
            StemmedWord { stemmed_offset: 0, suffix_offset: 5, suffix_len: 0,
                          stemmed: String::from("these"), suffix: String::from("") },
            StemmedWord { stemmed_offset: 6, suffix_offset: 10, suffix_len: 1,
                          stemmed: String::from("word"), suffix: String::from("s") },
            // "deeply" stems to "deepli"
            StemmedWord { stemmed_offset: 12, suffix_offset: 17, suffix_len: 1,
                          stemmed: String::from("deepli"), suffix: String::from("y") },
            StemmedWord { stemmed_offset: 19, suffix_offset: 23, suffix_len: 0,
                          stemmed: String::from("test"), suffix: String::from("") },
            StemmedWord { stemmed_offset: 24, suffix_offset: 30, suffix_len: 2,
                          stemmed: String::from("smooth"), suffix: String::from("ly") },
            StemmedWord { stemmed_offset: 33, suffix_offset: 37, suffix_len: 0,
                          stemmed: String::from("that"), suffix: String::from("") },
            StemmedWord { stemmed_offset: 38, suffix_offset: 42, suffix_len: 4,
                          stemmed: String::from("stem"), suffix: String::from("ming") },
            ];
        assert_eq!(result.len(), expected.len());
        for (stem, expected_stem) in result.iter().zip(expected.iter()) {
            assert_eq!(stem, expected_stem);
        }
    }

    #[test]
    fn test_stems_mixedcase() {
        let input = "THEse Words deeplY test smOOthly that stemmING";
        let stems = Stems::new(input);
        let result = Stems::new(input).collect::<Vec<StemmedWord>>();
        let expected = vec![
            StemmedWord { stemmed_offset: 0, suffix_offset: 5, suffix_len: 0,
                          stemmed: String::from("these"), suffix: String::from("") },
            StemmedWord { stemmed_offset: 6, suffix_offset: 10, suffix_len: 1,
                          stemmed: String::from("word"), suffix: String::from("s") },
            // "deeply" stems to "deepli"
            StemmedWord { stemmed_offset: 12, suffix_offset: 17, suffix_len: 1,
                          stemmed: String::from("deepli"), suffix: String::from("y") },
            StemmedWord { stemmed_offset: 19, suffix_offset: 23, suffix_len: 0,
                          stemmed: String::from("test"), suffix: String::from("") },
            StemmedWord { stemmed_offset: 24, suffix_offset: 30, suffix_len: 2,
                          stemmed: String::from("smooth"), suffix: String::from("ly") },
            StemmedWord { stemmed_offset: 33, suffix_offset: 37, suffix_len: 0,
                          stemmed: String::from("that"), suffix: String::from("") },
            StemmedWord { stemmed_offset: 38, suffix_offset: 42, suffix_len: 4,
                          stemmed: String::from("stem"), suffix: String::from("ming") },
            ];
        assert_eq!(result.len(), expected.len());
        for (stem, expected_stem) in result.iter().zip(expected.iter()) {
            assert_eq!(stem, expected_stem);
        }
    }

    #[test]
    fn test_stems_nonchars() {
        let input = "  1234 \t89";
        let stems = Stems::new(input);
        let result = Stems::new(input).collect::<Vec<StemmedWord>>();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_stems_some_nonchars() {
        let input = "534   Let's seeing89";
        let result = Stems::new(input).collect::<Vec<StemmedWord>>();
        let expected = vec![
            StemmedWord { stemmed_offset: 6, suffix_offset: 9, suffix_len: 0,
                          stemmed: String::from("let"), suffix: String::from("") },
            StemmedWord { stemmed_offset: 10, suffix_offset: 11, suffix_len: 0,
                          stemmed: String::from("s"), suffix: String::from("") },
            StemmedWord { stemmed_offset: 12, suffix_offset: 15, suffix_len: 3,
                          stemmed: String::from("see"), suffix: String::from("ing") },
            ];
        assert_eq!(result.len(), expected.len());
        for (stem, expected_stem) in result.iter().zip(expected.iter()) {
            assert_eq!(stem, expected_stem);
        }
    }

    #[test]
    fn test_stems_unicode() {
        let input = "Ünicöde stemming";
        let result = Stems::new(input).collect::<Vec<StemmedWord>>();
        let expected = vec![
            StemmedWord { stemmed_offset: 0, suffix_offset: 8, suffix_len: 1,
                          stemmed: String::from("ünicöd"), suffix: String::from("e") },
            StemmedWord { stemmed_offset: 10, suffix_offset: 14, suffix_len: 4,
                          stemmed: String::from("stem"), suffix: String::from("ming") },
            ];
        assert_eq!(result.len(), expected.len());
        for (stem, expected_stem) in result.iter().zip(expected.iter()) {
            assert_eq!(stem, expected_stem);
        }
    }

    #[test]
    fn test_stems_unicode_lowercase_has_more_bytes() {
        let input = "İ";
        println!("{} {} {} {}", input, input.to_lowercase(), input.len(), input.to_lowercase().len());
        let stems = Stems::new(input);
        for stemmed in stems {
            println!("stemmed: {:?}", stemmed);
        }
        let result = Stems::new(input).collect::<Vec<StemmedWord>>();
        let expected = vec![
            StemmedWord { stemmed_offset: 0, suffix_offset: 3, suffix_len: 0,
                          stemmed: String::from("i̇"), suffix: String::from("") },
            ];
        assert_eq!(result.len(), expected.len());
        for (stem, expected_stem) in result.iter().zip(expected.iter()) {
            assert_eq!(stem, expected_stem);
        }
    }

    // NOTE vmx 2016-10-21: Those cases exist in theory, but (language independent) it doesn't
    // seem to be the case (after reading
    // ftp://ftp.unicode.org/Public/UCD/latest/ucd/SpecialCasing.txt ). Some characters don't have
    // a corresponding uppercase chararacter with the same byte size, but when those uppercase
    // letters are transformed, to lowercase, they are transformed to the same number of bytes
    // combining several characters (although there is a single character for it).
    // Here's an example in Python:
    //
    //     >>> u"Ρ̓".lower() == u"ῤ"
    //     False
    //     >>> u"Ρ̓".lower() == u"ῤ".upper().lower()
    //     True
    // Or Rust:
    //
    //    let upper = "Ρ̓";
    //    let lower = "ῤ";
    //    println!("lower({}) == {}: {}", upper, lower, upper.to_lowercase() == lower);
    //    println!("lower({}) == lower(upper({})): {}", upper, lower, upper.to_lowercase() == lower.to_uppercase().to_lowercase());
    //    lower(Ρ̓) == ῤ: false
    //    lower(Ρ̓) == lower(upper(ῤ)): true
    //#[test]
    //fn test_stems_unicode_lowercase_has_less_bytes() {
    //}

    #[test]
    fn test_common_prefix_len() {
        let tests = vec![
            ("a", "a", 1),
            ("ab", "a", 1),
            ("a", "ab", 1),
            ("ab", "ab", 2),
            ("a", "b", 0),
            ("b", "a", 0),
            ("ab", "cd", 0),
            ("ab", "bc", 0),
            ("abc", "abd", 2),
            ("ac", "abcd", 1),
            (" a", "a", 0),
            ("a", "a ", 1),
            ("xyzabc", "xyz", 3),
            ("xyz", "xyzabc", 3),
            ("öxyz", "öx", 3),
            ];
        for (aa, bb, expected) in tests {
            let prefix_len = Stems::common_prefix_len(aa, bb);
            assert_eq!(prefix_len, expected);
        }
    }
}
