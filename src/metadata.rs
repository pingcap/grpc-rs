// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::grpc_sys::{self, grpc_metadata, grpc_metadata_array};
use std::borrow::Cow;
use std::mem::ManuallyDrop;
use std::{mem, slice, str};

use crate::error::{Error, Result};

const BINARY_ERROR_DETAILS_KEY: &str = "grpc-status-details-bin";

fn normalize_key(key: &str, binary: bool) -> Result<Cow<'_, str>> {
    if key.is_empty() {
        return Err(Error::InvalidMetadata(
            "metadata key should not be empty".to_owned(),
        ));
    }
    let mut is_upper_case = false;
    for b in key.as_bytes() {
        let b = *b;
        if (b'A'..=b'Z').contains(&b) {
            is_upper_case = true;
            continue;
        } else if (b'a'..=b'z').contains(&b)
            || (b'0'..=b'9').contains(&b)
            || b == b'_'
            || b == b'-'
            || b == b'.'
        {
            continue;
        }
        return Err(Error::InvalidMetadata(format!("key {:?} is invalid", key)));
    }
    let key = if is_upper_case {
        Cow::Owned(key.to_ascii_lowercase())
    } else {
        Cow::Borrowed(key)
    };
    if binary {
        if !key.as_bytes().ends_with(b"-bin") {
            return Err(Error::InvalidMetadata(
                "binary key should end with '-bin'".to_owned(),
            ));
        }
    } else if key.as_bytes().ends_with(b"-bin") {
        return Err(Error::InvalidMetadata(
            "non-binary key should not end with '-bin'".to_owned(),
        ));
    }
    Ok(key)
}

/// Builder for immutable Metadata.
pub struct MetadataBuilder {
    arr: Metadata,
}

impl MetadataBuilder {
    /// Create a builder with empty initial capacity.
    pub fn new() -> MetadataBuilder {
        MetadataBuilder::with_capacity(0)
    }

    /// Create a builder with the given value.
    pub fn with_capacity(cap: usize) -> MetadataBuilder {
        MetadataBuilder {
            arr: Metadata::with_capacity(cap),
        }
    }

    /// Add a metadata holding an ASCII value.
    ///
    /// `key` must not use suffix (-bin) indicating a binary valued metadata entry.
    pub fn add_str(&mut self, key: &str, value: &str) -> Result<&mut MetadataBuilder> {
        if !value.is_ascii() {
            return Err(Error::InvalidMetadata(
                "only ascii value is accepted.".to_owned(),
            ));
        }
        for b in value.bytes() {
            if 0 == unsafe { libc::isprint(b as i32) } {
                return Err(Error::InvalidMetadata(
                    "Only printable chars are accepted.".to_owned(),
                ));
            }
        }
        let key = normalize_key(key, false)?;
        Ok(self.add_metadata(&key, value.as_bytes()))
    }

    fn add_metadata(&mut self, key: &str, value: &[u8]) -> &mut MetadataBuilder {
        unsafe {
            grpc_sys::grpcwrap_metadata_array_add(
                &mut self.arr.0,
                key.as_ptr() as _,
                key.len(),
                value.as_ptr() as _,
                value.len(),
            )
        }
        self
    }

    /// Add a metadata holding a binary value.
    ///
    /// `key` needs to have suffix (-bin) indicating a binary valued metadata entry.
    pub fn add_bytes(&mut self, key: &str, value: &[u8]) -> Result<&mut MetadataBuilder> {
        let key = normalize_key(key, true)?;
        Ok(self.add_metadata(&key, value))
    }

    /// Set binary error details to support rich error model.
    ///
    /// See also https://grpc.io/docs/guides/error/#richer-error-model.
    pub(crate) fn set_binary_error_details(&mut self, value: &[u8]) -> &mut MetadataBuilder {
        self.add_metadata(BINARY_ERROR_DETAILS_KEY, value)
    }

    /// Create `Metadata` with configured entries.
    pub fn build(mut self) -> Metadata {
        unsafe {
            grpc_sys::grpcwrap_metadata_array_shrink_to_fit(&mut self.arr.0);
        }
        self.arr
    }
}

/// A collection of metadata entries that can be exchanged during a call.
///
/// gRPC supports these types of metadata:
///
/// - Request headers
///
///     They are sent by the client at the beginning of a remote call before
///     any request messages are sent.
///
/// - Response headers
///
///     They are sent by the server at the beginning of a remote call handler
///     before any response messages are sent.
///
/// - Response trailers
///
///     They are sent by the server at the end of a remote call along with
///     resulting call status.
///
/// Metadata value can be ascii string or bytes. They are distinguish by the
/// key suffix, key of bytes value should have suffix '-bin'.
#[repr(C)]
pub struct Metadata(grpc_metadata_array);

impl Metadata {
    fn with_capacity(cap: usize) -> Metadata {
        unsafe {
            let mut arr = mem::MaybeUninit::uninit();
            grpc_sys::grpcwrap_metadata_array_init(arr.as_mut_ptr(), cap);
            Metadata(arr.assume_init())
        }
    }

    /// Returns the count of metadata entries.
    #[inline]
    pub fn len(&self) -> usize {
        self.0.count
    }

    /// Returns true if there is no metadata entries.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.count == 0
    }

    /// Returns the metadata entry at the `index`.
    ///
    /// `None` is returned if out of bound.
    pub fn get(&self, index: usize) -> Option<(&str, &[u8])> {
        if self.0.count <= index {
            return None;
        }
        let (mut key_len, mut val_len) = (0, 0);
        unsafe {
            let key = grpc_sys::grpcwrap_metadata_array_get_key(&self.0, index, &mut key_len);
            let val = grpc_sys::grpcwrap_metadata_array_get_value(&self.0, index, &mut val_len);
            let key_str = str::from_utf8_unchecked(slice::from_raw_parts(key as _, key_len));
            let val_bytes = slice::from_raw_parts(val as *const u8, val_len);
            Some((key_str, val_bytes))
        }
    }

    /// Returns an iterator over the metadata entries.
    pub fn iter(&self) -> MetadataIter<'_> {
        MetadataIter {
            data: self,
            index: 0,
        }
    }

    /// Decomposes a Metadata array into its raw components.
    ///
    /// Returns the raw pointer to the underlying data, the length of the vector (in elements),
    /// and the allocated capacity of the data (in elements). These are the same arguments in
    /// the same order as the arguments to from_raw_parts.
    ///
    /// After calling this function, the caller is responsible for the memory previously managed
    /// by the Metadata. The only way to do this is to convert the raw pointer, length, and
    /// capacity back into a Metadata with the from_raw_parts function, allowing the destructor
    /// to perform the cleanup.
    pub fn into_raw_parts(self) -> (*mut grpc_metadata, usize, usize) {
        let s = ManuallyDrop::new(self);
        (s.0.metadata, s.0.count, s.0.capacity)
    }

    /// Creates a Metadata directly from the raw components of another vector.
    ///
    /// ## Safety
    ///
    /// The operation is safe only if the three arguments are returned from `into_raw_parts`
    /// and only convert once.
    pub unsafe fn from_raw_parts(p: *mut grpc_metadata, len: usize, cap: usize) -> Metadata {
        Metadata(grpc_metadata_array {
            count: len,
            capacity: cap,
            metadata: p,
        })
    }

    /// Search for binary error details.
    pub(crate) fn search_binary_error_details(&self) -> &[u8] {
        for (k, v) in self.iter() {
            if k == BINARY_ERROR_DETAILS_KEY {
                return v;
            }
        }
        &[]
    }
}

impl Clone for Metadata {
    fn clone(&self) -> Metadata {
        let mut builder = MetadataBuilder::with_capacity(self.len());
        for (k, v) in self.iter() {
            // use `add_metadata` to skip validation.
            builder.add_metadata(k, v);
        }
        builder.build()
    }
}

impl Drop for Metadata {
    fn drop(&mut self) {
        unsafe {
            grpc_sys::grpcwrap_metadata_array_cleanup(&mut self.0);
        }
    }
}

impl From<*const grpc_metadata_array> for Metadata {
    fn from(src: *const grpc_metadata_array) -> Metadata {
        // Create a no-destructor Metadata from a shallow copy of src
        let src = ManuallyDrop::new(Metadata(unsafe { *src }));

        // Return a deep copy of that wrapped shallow copy
        (*src).clone()
    }
}

unsafe impl Send for Metadata {}

/// Immutable metadata iterator
///
/// This struct is created by the iter method on `Metadata`.
pub struct MetadataIter<'a> {
    data: &'a Metadata,
    index: usize,
}

impl<'a> Iterator for MetadataIter<'a> {
    type Item = (&'a str, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.data.get(self.index);
        if res.is_some() {
            self.index += 1;
        }
        res
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remain = self.data.0.count - self.index;
        (remain, Some(remain))
    }
}

impl<'a> IntoIterator for &'a Metadata {
    type IntoIter = MetadataIter<'a>;
    type Item = (&'a str, &'a [u8]);

    fn into_iter(self) -> MetadataIter<'a> {
        MetadataIter {
            data: self,
            index: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_check() {
        let mut builder = MetadataBuilder::new();
        // Non-byte key should not end with '-bin'.
        assert!(builder.add_str("key-bin", "value").is_err());
        // Byte key should end with '-bin'.
        assert!(builder.add_bytes("key", b"value").is_err());
        // Key should not be empty.
        assert!(builder.add_str("", "value").is_err());
        // Key should follow the rule ^[a-z0-9_-.]+$
        assert!(builder.add_str(":key", "value").is_err());
        assert!(builder.add_str("key~", "value").is_err());
        assert!(builder.add_str("ke+y", "value").is_err());
        // Only printable ascii value is accepted when `add_str`.
        assert!(builder.add_str("key", "❤").is_err());
        assert!(builder.add_str("key", "\0").is_err());
        assert!(builder.add_str("key", "\n").is_err());

        builder.add_str("key", "value").unwrap();
        builder.add_str("_", "value").unwrap();
        builder.add_str("-", "value").unwrap();
        builder.add_str(".", "value").unwrap();
        builder.add_bytes("key-bin", b"value").unwrap();
    }

    #[test]
    fn test_metadata() {
        let mut builder = MetadataBuilder::new();
        let mut meta_kvs = vec![];
        for i in 0..5 {
            let key = format!("K{}", i);
            let val = format!("v{}", i);
            builder.add_str(&key, &val).unwrap();
            meta_kvs.push((key.to_ascii_lowercase(), val.into_bytes()));
        }
        for i in 5..10 {
            let key = format!("k{}-Bin", i);
            let val = format!("v{}", i);
            builder.add_bytes(&key, val.as_bytes()).unwrap();
            meta_kvs.push((key.to_ascii_lowercase(), val.into_bytes()));
        }
        let metadata = builder.build();
        for (i, (exp, res)) in meta_kvs.iter().zip(&metadata).enumerate() {
            let kv = metadata.get(i).unwrap();
            assert_eq!(kv, res);
            assert_eq!(res, (exp.0.as_str(), exp.1.as_slice()));
        }
        assert!(metadata.get(10).is_none());
        assert_eq!(metadata.len(), 10);
        assert!(!metadata.is_empty());
        {
            let mut iter = metadata.iter();
            for i in 0..10 {
                assert_eq!(iter.size_hint(), (10 - i, Some(10 - i)));
                iter.next();
            }
            assert_eq!(iter.size_hint(), (0, Some(0)));
        }

        let metadata1 = metadata.clone();
        for (x, y) in metadata.iter().zip(&metadata1) {
            assert_eq!(x, y);
        }
        drop(metadata);
        // Ensure deep copy.
        assert!(metadata1.get(0).is_some());

        let empty_metadata = MetadataBuilder::new().build();
        assert!(empty_metadata.is_empty());
        assert_eq!(empty_metadata.len(), 0);
    }
}
