extern crate stemmer;
extern crate unicode_normalization;
extern crate unicode_segmentation;

use self::stemmer::Stemmer;
use self::unicode_normalization::UnicodeNormalization;
use self::unicode_segmentation::UnicodeSegmentation;


pub struct Stems<'a> {
    words: unicode_segmentation::UWordBoundIndices<'a>,
    stemmer: Stemmer,
    word_position: usize,
}

#[derive(Debug, PartialEq)]
pub struct StemmedWord {
    // Where the stemmed word starts
    pub word_pos: u32,
    // The stemmed word
    pub stemmed: String,
}


impl<'a> Stems<'a> {
    pub fn new(text: &str) -> Stems {
        Stems {
            words: text.split_word_bound_indices(),
            stemmer: Stemmer::new("english").unwrap(),
            word_position: 0,
        }
    }
}

impl<'a> Iterator for Stems<'a> {
    type Item = StemmedWord;

    fn next(&mut self) -> Option<StemmedWord> {
        // we loop though until we find alphabetic chars. That becomes our stem word.
        let mut non_alpha = String::new(); // will contain any non-alphabetic chars
        // returned iff no other alphabetic chars
        while let Some((_pos, word)) = self.words.next() {
            let normalized = word.nfkc().collect::<String>();
            if normalized.chars().next().unwrap().is_alphabetic() {
                let pos = self.word_position;
                self.word_position += 1;
                return Some(StemmedWord {
                                word_pos: pos as u32,
                                stemmed: self.stemmer.stem(&normalized.to_lowercase()),
                            });
            } else {
                if self.word_position == 0 {
                    non_alpha.push_str(&normalized);
                }
            }
        }
        if non_alpha.is_empty() {
            if self.word_position == 0 {
                self.word_position = 1;
                // in this case we were passed an empty string
                // so we don't just return None, but we return
                // an empty string Stemmed word.
                // otherwise searching fields for empty strings
                // wouldn't be possible.
                return Some(StemmedWord {
                                word_pos: 0,
                                stemmed: String::new(),
                            });
            } else {
                return None;
            }
        } else {
            if self.word_position == 0 {
                self.word_position = 1;
                return Some(StemmedWord {
                                word_pos: 0,
                                stemmed: non_alpha,
                            });
            } else {
                return None;
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::{StemmedWord, Stems};

    #[test]
    fn test_stems_mixedcase() {
        let input = "THEse Words deeplY test smOOthly that stemmING";
        let result = Stems::new(input).collect::<Vec<StemmedWord>>();
        let expected = vec![StemmedWord {
                                word_pos: 0,
                                stemmed: String::from("these"),
                            },
                            StemmedWord {
                                word_pos: 1,
                                stemmed: String::from("word"),
                            },
                            // "deeply" stems to "deepli"
                            StemmedWord {
                                word_pos: 2,
                                stemmed: String::from("deepli"),
                            },
                            StemmedWord {
                                word_pos: 3,
                                stemmed: String::from("test"),
                            },
                            StemmedWord {
                                word_pos: 4,
                                stemmed: String::from("smooth"),
                            },
                            StemmedWord {
                                word_pos: 5,
                                stemmed: String::from("that"),
                            },
                            StemmedWord {
                                word_pos: 6,
                                stemmed: String::from("stem"),
                            }];
        assert_eq!(result.len(), expected.len());
        for (stem, expected_stem) in result.iter().zip(expected.iter()) {
            assert_eq!(stem, expected_stem);
        }
    }

    #[test]
    fn test_stems_nonchars() {
        let input = "  @#$!== \t+-";
        let result = Stems::new(input).collect::<Vec<StemmedWord>>();
        assert_eq!(result,
                   vec![StemmedWord {
                            word_pos: 0,
                            stemmed: String::from("  @#$!== \t+-"),
                        }]);
    }

    #[test]
    fn test_stems_some_nonchars() {
        let input = "@!?   Let's seeing...";
        let result = Stems::new(input).collect::<Vec<StemmedWord>>();
        let expected = vec![StemmedWord {
                                word_pos: 0,
                                stemmed: String::from("let"),
                            },
                            StemmedWord {
                                word_pos: 1,
                                stemmed: String::from("see"),
                            }];
        assert_eq!(result.len(), expected.len());
        for (stem, expected_stem) in result.iter().zip(expected.iter()) {
            assert_eq!(stem, expected_stem);
        }
    }

    #[test]
    fn test_stems_unicode() {
        let input = "Ünicöde stemming";
        let result = Stems::new(input).collect::<Vec<StemmedWord>>();
        let expected = vec![StemmedWord {
                                word_pos: 0,
                                stemmed: String::from("ünicöd"),
                            },
                            StemmedWord {
                                word_pos: 1,
                                stemmed: String::from("stem"),
                            }];
        assert_eq!(result.len(), expected.len());
        for (stem, expected_stem) in result.iter().zip(expected.iter()) {
            assert_eq!(stem, expected_stem);
        }
    }

    #[test]
    fn test_stems_trailing_needs_normalized() {
        let input = r#"Didgeridoos™"#;
        let result = Stems::new(input).collect::<Vec<StemmedWord>>();
        let expected = vec![StemmedWord {
                                word_pos: 0,
                                stemmed: String::from("didgeridoo"),
                            },
                            StemmedWord {
                                word_pos: 1,
                                stemmed: String::from("tm"),
                            }];
        assert_eq!(result.len(), expected.len());
        for (stem, expected_stem) in result.iter().zip(expected.iter()) {
            assert_eq!(stem, expected_stem);
        }
    }

    #[test]
    fn test_stems_unicode_lowercase_has_more_bytes() {
        let input = "İ";
        let result = Stems::new(input).collect::<Vec<StemmedWord>>();
        let expected = vec![StemmedWord {
                                word_pos: 0,
                                stemmed: String::from("i̇"),
                            }];
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
    //    println!("lower({}) == lower(upper({})): {}", upper, lower, upper.to_lowercase() ==
    //             lower.to_uppercase().to_lowercase());
    //    lower(Ρ̓) == ῤ: false
    //    lower(Ρ̓) == lower(upper(ῤ)): true
    #[test]
    fn test_stems_unicode_lowercase_has_less_bytes() {
        // The input is: Ρ̓ῤῤ (11 bytes), lowercases is ῤῤῤ (9 bytes)
        let input = "\u{03A1}\u{0313}\u{03C1}\u{0313}\u{1FE4}";
        let result = Stems::new(input).collect::<Vec<StemmedWord>>();
        let expected = vec![StemmedWord {
                                word_pos: 0,
                                stemmed: String::from("\u{03C1}\u{0313}\u{1FE4}\u{1FE4}"),
                            }];
        assert_eq!(result.len(), expected.len());
        for (stem, expected_stem) in result.iter().zip(expected.iter()) {
            assert_eq!(stem, expected_stem);
        }
    }
}
