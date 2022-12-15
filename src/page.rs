use crate::{
    error::Error,
    page_layout::{PAGE_SIZE, PTR_SIZE},
};

#[derive(Clone)]
pub struct Page {
    data: Box<[u8; PAGE_SIZE]>,
}

impl Page {
    pub fn new(data: [u8; PAGE_SIZE]) -> Self {
        Self {
            data: Box::new(data),
        }
    }

    pub fn new_empty() -> Self {
        Self::new([0; PAGE_SIZE])
    }

    /// get_data returns the underlying array.
    pub fn get_data(&self) -> [u8; PAGE_SIZE] {
        *self.data
    }

    /// gets a usize from some offset
    pub fn get_usize_from_offset(&self, offset: usize) -> Result<usize, Error> {
        if offset >= PAGE_SIZE - PTR_SIZE {
            return Err(Error::UnexpectedError(
                "Outside of page when getting usize".to_owned(),
            ));
        }

        let bytes = &self.data[offset..offset + PTR_SIZE];
        Ok(usize::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }
}

impl From<[u8; PAGE_SIZE]> for Page {
    fn from(data: [u8; PAGE_SIZE]) -> Self {
        Self::new(data)
    }
}
