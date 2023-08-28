use std::convert::TryInto;
use std::env;
use std::error::Error;
use std::fs::File;
use std::io;
use std::num::NonZeroUsize;
use std::slice;

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

fn count_lines_buf(buf: &[u8]) -> usize {
    bytecount::count(buf, b'\n')
}

const BUF_SIZE: usize = 32768;

fn count_lines_parallel<R: io::Read + std::os::fd::AsFd + std::os::unix::io::AsRawFd>(
    r: R,
    file_size: usize,
) -> Result<usize, Box<dyn Error>> {
    let num_chunks = num_cpus::get();
    if num_chunks == 1 {
        return count_lines_sequential(r);
    }

    let ptr = unsafe {
        nix::sys::mman::mmap(
            None,
            NonZeroUsize::new(file_size).unwrap(),
            nix::sys::mman::ProtFlags::PROT_READ,
            nix::sys::mman::MapFlags::MAP_PRIVATE,
            Some(r),
            0,
        )?
    };

    let data = unsafe { slice::from_raw_parts(ptr as *const u8, file_size) };

    let chunk_size = file_size / num_chunks;
    let mut chunks = (0..num_chunks - 1)
        .map(|i| (i * chunk_size..(i + 1) * chunk_size))
        .collect::<Vec<_>>();
    chunks.push((num_chunks - 1) * chunk_size..file_size);
    let count = chunks
        .par_iter()
        .map(|range| count_lines_buf(&data[range.start..range.end]))
        .sum();

    unsafe {
        nix::sys::mman::munmap(ptr, file_size)?;
    }

    Ok(count)
}

fn count_lines_sequential<R: io::Read + std::os::unix::io::AsRawFd>(
    mut r: R,
) -> Result<usize, Box<dyn Error>> {
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

fn count_lines<R: io::Read + std::os::fd::AsFd + std::os::unix::io::AsRawFd>(
    r: R,
) -> Result<usize, Box<dyn Error>> {
    // Use these even for parallel reads, since what it's actually doing is
    // telling the kernel "perform larger read-aheads on underlying block
    // device and put that in the page cache", which works fine with our pread
    // pattern.
    nix::fcntl::posix_fadvise(
        r.as_raw_fd(),
        0,
        0,
        nix::fcntl::PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL,
    )?;

    let st = nix::sys::stat::fstat(r.as_raw_fd())?;
    if nix::sys::stat::SFlag::from_bits_truncate(st.st_mode)
        .contains(nix::sys::stat::SFlag::S_IFREG)
    {
        return count_lines_parallel(r, st.st_size.try_into().unwrap());
    }

    count_lines_sequential(r)
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().skip(1);
    let num_args = args.len();

    if num_args == 0 {
        let lines = count_lines(io::stdin().lock())?;
        println!("{lines}");
    } else {
        let mut total_lines = 0;
        for path in args {
            let lines = count_lines(File::open(&path)?)?;
            total_lines += lines;
            println!("{lines} {path}");
        }
        if num_args > 1 {
            println!("{total_lines} total");
        }
    }

    Ok(())
}
