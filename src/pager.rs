use crate::{
    error::Error,
    page::Page,
    page_layout::{PAGE_SIZE, PTR_SIZE},
};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

#[derive(Clone, Eq, PartialEq, PartialOrd, Ord, Debug)]
pub struct Offset(pub usize);

#[derive(Clone, Eq, PartialEq, PartialOrd, Ord, Debug)]
pub struct ObjectAddress(pub usize);

impl From<usize> for Offset {
    fn from(v: usize) -> Self {
        Self(v)
    }
}

pub struct Pager {
    file: File,
    pages_allocated: usize,
    curser: usize,
    pub(crate) config: Config,
}

impl Pager {
    pub fn open(fp: &Path) -> Result<Self, Error> {
        let fd = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            // .truncate(true)
            .open(fp)?;

        let file_len = fd.metadata()?.len() as usize;
        let mut s = Self {
            file: fd,
            pages_allocated: file_len / PAGE_SIZE,
            curser: 0,
            config: Config::default(),
        };

        // println!("Pages allocated: {}", s.pages_allocated);

        if s.pages_allocated != 0 {
            s.config = Config::try_from(s.get_page(&Offset(0))?)?;
        } else {
            s.write_config()?;
        }
        // Get the cursor based on how long the file is
        // TODO: Replace the cursor with gc
        s.curser = s.file.metadata()?.len() as usize;

        Ok(s)
    }

    pub fn get_file_size(&self) -> Result<u64, Error> {
        Ok(self.file.metadata()?.len())
    }

    // TODO: Probably should have a function which does not read the full page
    pub fn get_page(&mut self, offset: &Offset) -> std::io::Result<Page> {
        let mut page: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        self.file.seek(SeekFrom::Start(offset.0 as u64))?;
        self.file.read_exact(&mut page)?;

        Ok(Page::new(page))
    }

    pub fn get_page_partial(&mut self, offset: &Offset, len: usize) -> std::io::Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(offset.0 as u64))?;

        let mut page = vec![0u8; len];
        self.file.read_exact(&mut page)?;

        Ok(page)
    }

    pub fn write_page(&mut self, page: &Page) -> Result<Offset, Error> {
        let offset = self.alloc_page()?;
        self.write_page_at_offset(&offset, page)?;
        Ok(offset)
    }

    pub fn write_page_at_offset(&mut self, offset: &Offset, page: &Page) -> Result<(), Error> {
        self.file.seek(SeekFrom::Start(offset.0 as u64))?;
        self.file.write_all(&page.get_data())?;

        Ok(())
    }

    fn write_page_at_offset_partial(&mut self, offset: u64, page: &[u8]) -> Result<(), Error> {
        assert!(page.len() < PAGE_SIZE);

        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&page)?;

        Ok(())
    }

    pub fn write_config(&mut self) -> Result<(), Error> {
        self.write_page_at_offset(&Offset(0), &Page::from(&self.config))
    }

    pub fn set_root_page(&mut self, root_page: Offset) -> Result<(), Error> {
        self.config.root_page = Some(root_page);
        self.write_config()
    }

    fn alloc_page(&mut self) -> Result<Offset, Error> {
        if let Some(ffp) = self.config.first_free_page.to_owned() {
            // Find the first free page after ffp
            let new_ffp = self.get_page(&ffp)?.get_usize_from_offset(0)?;
            // If there is none, we set first_free_page to None
            // Otherwise we use the next offset as our new ffp
            self.config.first_free_page = if new_ffp == 0 {
                None
            } else {
                Some(Offset(new_ffp))
            };

            Ok(ffp)
        } else {
            // If there is no available free page, we need to expand our file
            // TODO: Here we assume the function using the allocated space is
            // TODO: writing to it right away, and a whole page at a time.
            // TODO: We may need to write 0s to the page - just to be safe!
            let alloc_ptr = self.curser;
            self.curser += PAGE_SIZE;
            Ok(Offset(alloc_ptr))
        }
    }

    /// Free multiple pages at once using a FreeQueue
    pub fn free_pages(&mut self, free_queue: FreeQueue) -> Result<(), Error> {
        // TODO: This can be optimized heavily...

        for o in free_queue.q() {
            self.free_page(&o)?;
        }

        Ok(())
    }

    pub fn free_page(&mut self, offset: &Offset) -> Result<(), Error> {
        if let Some(ffp) = self.config.first_free_page.to_owned() {
            if ffp > *offset {
                // Update the new first free page to be a pointer to the old one
                // and to the 0th page
                self.write_page_at_offset_partial(
                    offset.0 as u64,
                    &[ffp.0.to_be_bytes(), [0_u8; 8]].concat(),
                )?;

                // Update first_free_page
                self.config.first_free_page = Some(offset.to_owned());
            } else {
                // Since the page to be free'd is after the first
                // one, we need to find where to put this one
                // TODO: Maybe optimize this to require less writes
                // TODO: Each free page could maybe hold a number of
                // TODO: Other free pages, in a tree like fashion...

                // Find the page to change and what offset to point to
                // let mut next_offset = self.get_page(&ffp)?.get_usize_from_offset(0)?;
                let mut next_offset =
                    read_be_usize(&mut &self.get_page_partial(&ffp, PTR_SIZE)?[..8]);

                let mut current_offset = ffp.0;
                loop {
                    if next_offset == 0 {
                        // Since next_offset is 0, current_offset must
                        // be the last free'd page, and must be updated
                        // to point to the newly free'd page
                        // self.write_page_at_offset(
                        //     &current_offset.into(),
                        //     &make_pointer_page(offset.0),
                        // )?;
                        self.write_page_at_offset_partial(
                            current_offset as u64,
                            &offset.0.to_be_bytes(),
                        )?;

                        // The free'd page is now the last free page: (->0)
                        self.write_page_at_offset(offset, &Page::new_empty())?;

                        return Ok(());
                    }

                    // Use the next offset if it is after the free'd page
                    if next_offset > offset.0 {
                        break;
                    }

                    current_offset = next_offset;
                    // next_offset = self
                    //     .get_page(&Offset(next_offset))?
                    //     .get_usize_from_offset(0)?;
                    next_offset = read_be_usize(
                        &mut &self.get_page_partial(&Offset(next_offset), PTR_SIZE)?[..8],
                    );
                }

                // Point the free'd page to the next offset
                // self.write_page_at_offset(offset, &make_pointer_page(next_offset))?;
                self.write_page_at_offset_partial(offset.0 as u64, &next_offset.to_be_bytes())?;

                // Point the page before to the free'd page
                // self.write_page_at_offset(&Offset(current_offset), &make_pointer_page(offset.0))?;
                self.write_page_at_offset_partial(current_offset as u64, &offset.0.to_be_bytes())?;
            }
        } else {
            // Since we have no previous free'd pages, we create
            // our first, referencing 0 (AKA last free page)
            self.write_page_at_offset(offset, &Page::new_empty())?;
            self.config.first_free_page = Some(offset.to_owned());
            self.write_config()?;
        }

        Ok(())
    }

    /// Write an object to disk and get the new offset
    pub fn write_object(&mut self, object: &Vec<u8>) -> Result<Offset, Error> {
        // TODO: This has to be more efficient in the future
        if object.len() > PAGE_SIZE - PTR_SIZE {
            todo!()
        }

        let mut data = [0_u8; PAGE_SIZE];
        data[0..PTR_SIZE].clone_from_slice(&object.len().to_be_bytes());
        data[PTR_SIZE..PTR_SIZE + object.len()].clone_from_slice(&object);
        self.write_page(&Page::from(data))
    }

    /// Free an object, deallocating the space
    pub fn free_object(&mut self, offset: Offset) -> Result<(), Error> {
        self.free_page(&offset)
    }

    /// Get an object from an offset
    pub fn get_object(&mut self, offset: &Offset) -> Result<Vec<u8>, Error> {
        let p = self.get_page(offset)?;
        let len = p.get_usize_from_offset(0)?;

        let out = p.get_data()[PTR_SIZE..PTR_SIZE + len].to_owned();

        Ok(out)
    }
}

// fn make_pointer_page(ptr: usize) -> Page {
//     let mut data = [0; PAGE_SIZE];
//     data[0..PTR_SIZE].clone_from_slice(&ptr.to_be_bytes());
//     Page::from(data)
// }

pub struct Config {
    pub(crate) root_page: Option<Offset>,
    first_free_page: Option<Offset>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            root_page: None,
            first_free_page: None,
        }
    }
}

impl TryFrom<Page> for Config {
    type Error = Error;
    fn try_from(page: Page) -> Result<Self, Self::Error> {
        let root_page = page.get_usize_from_offset(0)?;
        let root_page = if root_page == 0 {
            None
        } else {
            Some(Offset(root_page))
        };

        let first_free_page = page.get_usize_from_offset(PTR_SIZE)?;
        let first_free_page = if first_free_page == 0 {
            None
        } else {
            Some(Offset(first_free_page))
        };

        Ok(Config {
            root_page,
            first_free_page,
        })
    }
}

impl From<&Config> for Page {
    fn from(cfg: &Config) -> Self {
        let mut data = [0x00; PAGE_SIZE];
        if let Some(rp) = &cfg.root_page {
            data[0..0 + PTR_SIZE].clone_from_slice(&rp.0.to_be_bytes());
        }
        if let Some(ffp) = &cfg.first_free_page {
            data[PTR_SIZE..2 * PTR_SIZE].clone_from_slice(&ffp.0.to_be_bytes());
        }

        Page::new(data)
    }
}

pub struct FreeQueue {
    q: Vec<Offset>,
}

impl FreeQueue {
    pub fn new() -> Self {
        Self { q: vec![] }
    }

    /// Add an offset to the free queue.
    /// self.q is kept sorted and without duplicates
    pub fn add(&mut self, offset: Offset) {
        if let Some(add_idx) = self.q.binary_search(&offset).err() {
            self.q.insert(add_idx, offset);
        }
    }

    pub fn q(self) -> Vec<Offset> {
        self.q
    }
}

mod test {
    #[test]
    fn test_free_queue() {
        use super::{FreeQueue, Offset};

        let mut fq = FreeQueue::new();
        fq.add(Offset(0));
        fq.add(Offset(10));
        fq.add(Offset(5));
        fq.add(Offset(15));
        fq.add(Offset(5));
        fq.add(Offset(10));
        fq.add(Offset(2));

        assert_eq!(
            fq.q(),
            vec![Offset(0), Offset(2), Offset(5), Offset(10), Offset(15)]
        );
    }
}

fn read_be_usize(input: &mut &[u8]) -> usize {
    let (int_bytes, rest) = input.split_at(std::mem::size_of::<usize>());
    *input = rest;
    usize::from_be_bytes(int_bytes.try_into().unwrap())
}
