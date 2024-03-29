Course project for CS 6913 at NYU, designed to process a large collection of text documents and use them to create an inverted index.

Project Components:

TRECProcessor: This class processes TREC (Text REtrieval Conference) files. It reads compressed TREC files, decompresses them, and passes individual documents to the DocumentParser for tokenization. Each document is processed only once.

DocumentParser: The DocumentParser class extracts URLs and text content from documents. It tokenizes the text, calculates term frequencies, and creates Postings objects. These Postings objects represent terms and their associated document IDs and frequencies.  The Postings are passed in a list to the IntermediatePostingGenerator.

IntermediatePostingGenerator: This class accumulates Postings, sorts them, serializes them, and writes them to intermediate files. The intermediate files are temporary and not yet in their final form.  The class uses an ArrayList as a buffer to accumulate postings.  When the ArrayList reaches a fixed size, the postings are sorted and written to a temporary file.

FileMerger: The FileMerger class merges sorted intermediate files, encodes document IDs and frequencies using variable-length byte encoding, and writes the final inverted index to a file. It also builds a lexicon that stores metadata about the terms in the index.
The program uses a variety of techniques, including file I/O, data compression, sorting, and encoding, to create the inverted index efficiently.
