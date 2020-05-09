# line-counter

High performance implementation of a subset of `wc -l`. Counts the number of `\n` that appear in a file... and nothing more.

Seems to be nearly 3x faster than `wc -l` on a 4.2GB CSV with 28 million lines.

Usage:

```console
$ command | line-counter
1234
$ line-counter file1.txt file2.txt
1234 file1.txt
4321 file2.txt
```
