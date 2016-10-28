extern crate stemmer;
extern crate unicode_normalization;
extern crate unicode_segmentation;


use self::stemmer::Stemmer;
use self::unicode_normalization::UnicodeNormalization;
use self::unicode_segmentation::UnicodeSegmentation;


pub struct Stems<'a> {
    words: unicode_segmentation::UnicodeWordIndices<'a>,
    stemmer: Stemmer,
}

#[derive(Debug, PartialEq)]
pub struct StemmedWord {
    // Where the stemmed word starts
    pub stemmed_offset: usize,
    // Where the suffix starts
    pub suffix_offset: usize,
    // The stemmed word
    pub stemmed: String,
    // The difference between the stemmed word and the original lowercased one. It can be
    // used to recontruct the original word (for exact match searches)
    pub suffix: String,
}


impl<'a> Stems<'a> {
    pub fn new(text: &str) -> Stems {
        Stems{
            words: text.unicode_word_indices(),
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
        match self.words.next() {
            Some((pos, word)) => {
                let lowercased = word.to_lowercase();
                let normalized = lowercased.nfkc().collect::<String>();
                let stemmed = self.stemmer.stem(&normalized);
                let prefix_len = Stems::common_prefix_len(&stemmed, &normalized);
                let ret = StemmedWord {
                    stemmed_offset: pos,
                    suffix_offset: pos + prefix_len,
                    stemmed: stemmed,
                    suffix: (&normalized[prefix_len..normalized.len()]).to_string(),
                };
                Some(ret)
            },
            None => None
        }
    }

}


#[cfg(test)]
mod tests {
    use super::{StemmedWord, Stems};

    #[test]
    fn test_stems_lowercase() {
        let input = "These words deeply test smoothly that stemming";
        let result = Stems::new(input).collect::<Vec<StemmedWord>>();
        let expected = vec![
            StemmedWord { stemmed_offset: 0, suffix_offset: 5,
                          stemmed: String::from("these"), suffix: String::from("") },
            StemmedWord { stemmed_offset: 6, suffix_offset: 10,
                          stemmed: String::from("word"), suffix: String::from("s") },
            // "deeply" stems to "deepli"
            StemmedWord { stemmed_offset: 12, suffix_offset: 17,
                          stemmed: String::from("deepli"), suffix: String::from("y") },
            StemmedWord { stemmed_offset: 19, suffix_offset: 23,
                          stemmed: String::from("test"), suffix: String::from("") },
            StemmedWord { stemmed_offset: 24, suffix_offset: 30,
                          stemmed: String::from("smooth"), suffix: String::from("ly") },
            StemmedWord { stemmed_offset: 33, suffix_offset: 37,
                          stemmed: String::from("that"), suffix: String::from("") },
            StemmedWord { stemmed_offset: 38, suffix_offset: 42,
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
        let result = Stems::new(input).collect::<Vec<StemmedWord>>();
        let expected = vec![
            StemmedWord { stemmed_offset: 0, suffix_offset: 5,
                          stemmed: String::from("these"), suffix: String::from("") },
            StemmedWord { stemmed_offset: 6, suffix_offset: 10,
                          stemmed: String::from("word"), suffix: String::from("s") },
            // "deeply" stems to "deepli"
            StemmedWord { stemmed_offset: 12, suffix_offset: 17,
                          stemmed: String::from("deepli"), suffix: String::from("y") },
            StemmedWord { stemmed_offset: 19, suffix_offset: 23,
                          stemmed: String::from("test"), suffix: String::from("") },
            StemmedWord { stemmed_offset: 24, suffix_offset: 30,
                          stemmed: String::from("smooth"), suffix: String::from("ly") },
            StemmedWord { stemmed_offset: 33, suffix_offset: 37,
                          stemmed: String::from("that"), suffix: String::from("") },
            StemmedWord { stemmed_offset: 38, suffix_offset: 42,
                          stemmed: String::from("stem"), suffix: String::from("ming") },
            ];
        assert_eq!(result.len(), expected.len());
        for (stem, expected_stem) in result.iter().zip(expected.iter()) {
            assert_eq!(stem, expected_stem);
        }
    }

    #[test]
    fn test_stems_nonchars() {
        let input = "  @#$!== \t+-";
        let result = Stems::new(input).collect::<Vec<StemmedWord>>();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_stems_some_nonchars() {
        let input = "@!?   Let's seeing...";
        let result = Stems::new(input).collect::<Vec<StemmedWord>>();
        let expected = vec![
            StemmedWord { stemmed_offset: 6, suffix_offset: 9,
                          stemmed: String::from("let"), suffix: String::from("'s") },
            StemmedWord { stemmed_offset: 12, suffix_offset: 15,
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
            StemmedWord { stemmed_offset: 0, suffix_offset: 8,
                          stemmed: String::from("ünicöd"), suffix: String::from("e") },
            StemmedWord { stemmed_offset: 10, suffix_offset: 14,
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
        let result = Stems::new(input).collect::<Vec<StemmedWord>>();
        let expected = vec![
            StemmedWord { stemmed_offset: 0, suffix_offset: 3,
                          stemmed: String::from("i̇"), suffix: String::from("") },
            ];
        assert_eq!(result.len(), expected.len());
        for (stem, expected_stem) in result.iter().zip(expected.iter()) {
            assert_eq!(stem, expected_stem);
        }
    }

    // Some characters don't have a corresponding uppercase chararacter with the same byte size,
    // but when those uppercase letters are transformed to lowercase, they are transformed to the
    // same number of bytes combining several characters (although there is a single character for
    // it).
    // Here's an example in Python:
    //
    //     >>> u"Ρ̓".lower() == u"ῤ"
    //     False
    //     >>> u"Ρ̓".lower() == u"ῤ".lower()
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
    #[test]
    fn test_stems_unicode_lowercase_has_less_bytes() {
        // The input is: Ρ̓ῤῤ (11 bytes), lowercases is ῤῤῤ (9 bytes)
        let input = "\u{03A1}\u{0313}\u{03C1}\u{0313}\u{1FE4}";
        let result = Stems::new(input).collect::<Vec<StemmedWord>>();
        let expected = vec![
            StemmedWord { stemmed_offset: 0, suffix_offset: 9,
                          stemmed: String::from("\u{1FE4}\u{1FE4}\u{1FE4}"), suffix: String::from("") },
           ];
        assert_eq!(result.len(), expected.len());
        for (stem, expected_stem) in result.iter().zip(expected.iter()) {
            assert_eq!(stem, expected_stem);
        }
    }

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
