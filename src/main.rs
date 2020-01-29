use std::convert::TryInto;
use std::error::Error;
use std::fs::File;
use std::io;
use std::path::PathBuf;

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(name = "line-counter")]
struct Opt {
    paths: Vec<PathBuf>,
}

fn count_lines_buf(buf: &[u8]) -> usize {
    let mut lines = 0;
    for _ in memchr::memchr_iter(b'\n', &buf) {
        lines += 1;
    }
    lines
}

const BUF_SIZE: usize = 32768;

fn count_lines_parallel<R: io::Read + std::os::unix::io::AsRawFd>(
    r: R,
    file_size: usize,
) -> Result<usize, Box<dyn Error>> {
    let num_chunks = num_cpus::get();
    if num_chunks == 1 {
        return count_lines_sequential(r);
    }

    let chunk_size = file_size / num_chunks;
    let mut chunks = (0..num_chunks - 1)
        .map(|i| (i * chunk_size..(i + 1) * chunk_size))
        .collect::<Vec<_>>();
    chunks.push((num_chunks - 1) * chunk_size..file_size);

    let raw_fd = r.as_raw_fd();
    Ok(chunks
        .par_iter()
        .map(|range| {
            let mut buf = [0u8; BUF_SIZE];
            let mut pos = range.start;
            let mut lines = 0;
            while pos < range.end {
                let n = nix::sys::uio::pread(
                    raw_fd,
                    &mut buf[..BUF_SIZE.min(range.end - pos)],
                    pos.try_into().unwrap(),
                )
                .unwrap();
                if n == 0 {
                    break;
                }
                lines += count_lines_buf(&buf[..n]);
                pos += n;
            }
            lines
        })
        .sum())
}

fn count_lines_sequential<R: io::Read + std::os::unix::io::AsRawFd>(
    mut r: R,
) -> Result<usize, Box<dyn Error>> {
    nix::fcntl::posix_fadvise(
        r.as_raw_fd(),
        0,
        0,
        nix::fcntl::PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL,
    )?;

    let mut buf = [0u8; BUF_SIZE];
    let mut lines = 0;
    loop {
        let n = r.read(&mut buf)?;
        if n == 0 {
            break;
        }
        lines += count_lines_buf(&buf[..n]);
    }

    Ok(lines)
}

fn count_lines<R: io::Read + std::os::unix::io::AsRawFd>(r: R) -> Result<usize, Box<dyn Error>> {
    let st = nix::sys::stat::fstat(r.as_raw_fd())?;
    if nix::sys::stat::SFlag::from_bits_truncate(st.st_mode)
        .contains(nix::sys::stat::SFlag::S_IFREG)
    {
        return count_lines_parallel(r, st.st_size.try_into().unwrap());
    }

    count_lines_sequential(r)
}

fn main() -> Result<(), Box<dyn Error>> {
    let opt = Opt::from_args();
    if opt.paths.is_empty() {
        let lines = count_lines(io::stdin().lock())?;
        println!("{}", lines);
    } else {
        let mut total_lines = 0;
        for path in &opt.paths {
            let lines = count_lines(File::open(&path)?)?;
            total_lines += lines;
            println!("{} {}", lines, path.to_string_lossy());
        }
        if opt.paths.len() > 1 {
            println!("{} total", total_lines);
        }
    }

    Ok(())
}
