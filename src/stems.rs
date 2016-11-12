extern crate stemmer;
extern crate unicode_normalization;
extern crate unicode_segmentation;

use std::iter::Peekable;

use self::stemmer::Stemmer;
use self::unicode_normalization::UnicodeNormalization;
use self::unicode_segmentation::UnicodeSegmentation;


pub struct Stems<'a> {
    words: Peekable<unicode_segmentation::UWordBoundIndices<'a>>,
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
            words: text.split_word_bound_indices().peekable(),
            stemmer: Stemmer::new("english").unwrap(),
        }
    }

    /// Return the *byte* length of the common prefix between two strings
    fn common_prefix_len(aa: &str, bb: &str) -> usize {
        aa.chars()
            .zip(bb.chars())
            .take_while(|&(a, b)| a == b)
            .fold(0, |acc, (a, _)| acc + a.len_utf8())
    }
}

impl<'a> Iterator for Stems<'a> {
    type Item = StemmedWord;

    fn next(&mut self) -> Option<StemmedWord> {
        let mut word_to_stem = String::new();
        let mut stemmed_offset = 0;
        let mut normalized = String::new();
        loop {
            match self.words.peek() {
                Some(&(pos, word)) => {
                    normalized = word.nfkc().collect::<String>();
                    if word.chars().next().unwrap().is_alphabetic() {
                        stemmed_offset = pos;
                        break;
                    } else {
                        word_to_stem.push_str(&normalized);
                        self.words.next();
                    }
                },
                None => {
                    if word_to_stem.is_empty() {
                        return None;
                    } else {
                        break;
                    }
                },
            }
        }

        if !word_to_stem.is_empty() {
            // we found the begining of the string is not a stemmable word.
            // Return the accumulated string as the stemmed word
            return Some(StemmedWord {
                            stemmed_offset: 0,
                            suffix_offset: word_to_stem.len(),
                            stemmed: word_to_stem,
                            suffix: String::new(),
                });
        }
        // normalized contains our stemmable word. advance the iter since we only peeked.
        self.words.next();
        word_to_stem = normalized;
        let mut suffix = word_to_stem.clone();
        loop {
            // loop through all non-alphabetic chars and add to suffix (which means the suffix starts
            // before the stemmed word)
            match self.words.peek() {
                Some(&(_pos, word)) => {
                    normalized = word.nfkc().collect::<String>();
                    if normalized.chars().next().unwrap().is_alphabetic() {
                        break;
                    } else {
                        suffix.push_str(&normalized);
                        self.words.next();
                    }
                },
                None => break,
            }
        }
        let stemmed = self.stemmer.stem(&word_to_stem.to_lowercase());
        let prefix_len = Stems::common_prefix_len(&stemmed, &suffix);
        Some(StemmedWord {
            stemmed_offset: stemmed_offset,
            suffix_offset: stemmed_offset + prefix_len,
            stemmed: stemmed,
            suffix: (&suffix[prefix_len..]).to_string(),
        })
    }
}


#[cfg(test)]
mod tests {
    use super::{StemmedWord, Stems};

    #[test]
    fn test_stems_mixedcase() {
        let input = "THEse Words deeplY test smOOthly that stemmING";
        let result = Stems::new(input).collect::<Vec<StemmedWord>>();
        let expected = vec![
            StemmedWord { stemmed_offset: 0, suffix_offset: 0,
                          stemmed: String::from("these"), suffix: String::from("THEse ") },
            StemmedWord { stemmed_offset: 6, suffix_offset: 6,
                          stemmed: String::from("word"), suffix: String::from("Words ") },
            // "deeply" stems to "deepli"
            StemmedWord { stemmed_offset: 12, suffix_offset: 17,
                          stemmed: String::from("deepli"), suffix: String::from("Y ") },
            StemmedWord { stemmed_offset: 19, suffix_offset: 23,
                          stemmed: String::from("test"), suffix: String::from(" ") },
            StemmedWord { stemmed_offset: 24, suffix_offset: 26,
                          stemmed: String::from("smooth"), suffix: String::from("OOthly ") },
            StemmedWord { stemmed_offset: 33, suffix_offset: 37,
                          stemmed: String::from("that"), suffix: String::from(" ") },
            StemmedWord { stemmed_offset: 38, suffix_offset: 42,
                          stemmed: String::from("stem"), suffix: String::from("mING") },
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
        assert_eq!(result, vec![
            StemmedWord { stemmed_offset: 0, suffix_offset: 12,
                          stemmed: String::from("  @#$!== \t+-"), suffix: String::from("") },
            ]);
    }

    #[test]
    fn test_stems_some_nonchars() {
        let input = "@!?   Let's seeing...";
        let result = Stems::new(input).collect::<Vec<StemmedWord>>();
        let expected = vec![
            StemmedWord { stemmed_offset: 0, suffix_offset: 6,
                          stemmed: String::from("@!?   "), suffix: String::from("") },
            StemmedWord { stemmed_offset: 6, suffix_offset: 6,
                          stemmed: String::from("let"), suffix: String::from("Let's ") },
            StemmedWord { stemmed_offset: 12, suffix_offset: 15,
                          stemmed: String::from("see"), suffix: String::from("ing...") },
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
            StemmedWord { stemmed_offset: 0, suffix_offset: 0,
                          stemmed: String::from("ünicöd"), suffix: String::from("Ünicöde ") },
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
            StemmedWord { stemmed_offset: 0, suffix_offset: 0,
                          stemmed: String::from("i̇"), suffix: String::from("İ") },
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
            StemmedWord { stemmed_offset: 0, suffix_offset: 0,
                          stemmed: String::from("\u{03C1}\u{0313}\u{1FE4}\u{1FE4}"),
                          suffix: String::from("\u{03A1}\u{0313}\u{1FE4}\u{1FE4}") },
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
