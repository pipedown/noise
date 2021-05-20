extern crate libc;
extern crate noise_search;

use std::ffi::{CStr, CString};

use libc::{c_char};
use noise_search::index::{Batch, Index, OpenOptions};
use noise_search::json_value::PrettyPrint;
use noise_search::query::QueryResults;

fn to_str<'a>(str: *const c_char) -> &'a str {
    let c_str = unsafe {
        assert!(!str.is_null());
        CStr::from_ptr(str)
    };

    c_str.to_str().unwrap()
}

#[no_mangle]
pub extern fn free_string(string: *mut c_char) {
    unsafe {
        if string.is_null() {
            return
        }
        CString::from_raw(string);
    };
}

#[no_mangle]
pub extern fn batch_new() -> *mut Batch {
    Box::into_raw(Box::new(Batch::new()))
}

#[no_mangle]
pub extern fn batch_free(batch_ptr: *mut Batch) {
    if batch_ptr.is_null() {
        println!("vmx: that case");
        return
    }
    unsafe { Box::from_raw(batch_ptr); };
}

#[no_mangle]
pub extern fn open(name_c: *const c_char) -> *mut Index {
    let name = to_str(name_c);
    println!("open index at {:?}", name);

    let index = Index::open(name, Some(OpenOptions::Create)).unwrap();
    Box::into_raw(Box::new(index))
}

#[no_mangle]
pub extern fn add(index_ptr: *mut Index, json_ptr: *const c_char, batch_ptr: *mut Batch) ->
        *mut c_char {
    let index = unsafe {
        assert!(!index_ptr.is_null());
        &mut *index_ptr
    };
    let json = to_str(json_ptr);
    let mut batch = unsafe {
        assert!(!batch_ptr.is_null());
        &mut *batch_ptr
    };

    let added = index.add(json, &mut batch).unwrap();
    let c_str_added = CString::new(added).unwrap();
    c_str_added.into_raw()
}

#[no_mangle]
pub extern fn flush(index_ptr: *mut Index, batch_ptr: *mut Batch) {
    let index = unsafe {
        assert!(!index_ptr.is_null());
        &mut *index_ptr
    };
    let batch = unsafe {
      assert!(!batch_ptr.is_null());
      Box::from_raw(batch_ptr)
    };
    index.flush(*batch).unwrap();
}

#[no_mangle]
pub extern fn query<'a>(index_ptr: *mut Index, query_ptr: *const c_char) -> *mut QueryResults<'a> {
    let index = unsafe {
        assert!(!index_ptr.is_null());
        &mut *index_ptr
    };
    let query = to_str(query_ptr);
    let query_results = index.query(query, None).unwrap();
    Box::into_raw(Box::new(query_results))
}

#[no_mangle]
pub extern fn query_results_next(query_results_ptr: *mut QueryResults) -> *mut c_char {
    let query_results = unsafe {
        assert!(!query_results_ptr.is_null());
        &mut *query_results_ptr
    };

    let mut pretty = PrettyPrint::new("", "", "");
    let mut buffer: Vec<u8> = Vec::new();
    match query_results.next() {
        Some(rr) => {
            rr.render(&mut buffer, &mut pretty).unwrap();

            let result_str = unsafe {
                std::str::from_utf8_unchecked(&buffer)
            };
            println!("query results next: {:?}", result_str);
            let result_cstr = CString::new(result_str).unwrap();
            result_cstr.into_raw()
        }
        None => {
            std::ptr::null_mut()
        }
    }
}
