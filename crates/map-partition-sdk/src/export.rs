/// Macro to export a `PartitionProcessor` as 5 C ABI functions for .so dynamic loading.
///
/// # Usage
///
/// ```rust,ignore
/// struct MyProcessor { ... }
/// impl PartitionProcessor for MyProcessor { ... }
/// export_partition_processor!(MyProcessor, my_processor);
/// ```
///
/// This generates the following exported functions:
/// - `my_processor_init` — partition initialization
/// - `my_processor_feed` — streaming input
/// - `my_processor_execute` — execute business logic
/// - `my_processor_fetch` — streaming output
/// - `my_processor_finish` — cleanup
#[macro_export]
macro_rules! export_partition_processor {
    ($processor_type:ty, $fn_name:ident) => {
        ::paste::paste! {
            #[unsafe(no_mangle)]
            pub extern "C" fn [<$fn_name _init>](
                schema_ptr: *const u8,
                schema_len: i64,
            ) -> *mut std::ffi::c_void {
                let schema_bytes =
                    unsafe { std::slice::from_raw_parts(schema_ptr, schema_len as usize) };
                let schema = match $crate::decode_schema(schema_bytes) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("[{}] init: failed to decode schema: {}", stringify!($fn_name), e);
                        return std::ptr::null_mut();
                    }
                };
                let processor = <$processor_type as $crate::PartitionProcessor>::new(schema);
                Box::into_raw(Box::new(processor)) as *mut std::ffi::c_void
            }

            #[unsafe(no_mangle)]
            pub extern "C" fn [<$fn_name _feed>](
                ctx: *mut std::ffi::c_void,
                input_ptr: *const u8,
                input_len: i64,
            ) -> i32 {
                let processor = unsafe { &mut *(ctx as *mut $processor_type) };
                let input_bytes =
                    unsafe { std::slice::from_raw_parts(input_ptr, input_len as usize) };
                match $crate::decode_batch(input_bytes) {
                    Ok(batch) => {
                        processor.feed(batch);
                        0
                    }
                    Err(e) => {
                        eprintln!("[{}] feed error: {}", stringify!($fn_name), e);
                        -1
                    }
                }
            }

            #[unsafe(no_mangle)]
            pub extern "C" fn [<$fn_name _execute>](
                ctx: *mut std::ffi::c_void,
            ) -> i32 {
                let processor = unsafe { &mut *(ctx as *mut $processor_type) };
                processor.execute();
                0
            }

            #[unsafe(no_mangle)]
            pub extern "C" fn [<$fn_name _fetch>](
                ctx: *mut std::ffi::c_void,
                output_ptr: *mut *mut u8,
                output_len: *mut i64,
            ) -> i32 {
                let processor = unsafe { &mut *(ctx as *mut $processor_type) };
                match processor.fetch() {
                    Some(batch) => {
                        match $crate::encode_batch(&batch) {
                            Ok(mut bytes) => {
                                let len = bytes.len();
                                let ptr = bytes.as_mut_ptr();
                                std::mem::forget(bytes);
                                unsafe {
                                    *output_ptr = ptr;
                                    *output_len = len as i64;
                                }
                                0 // more data may be available
                            }
                            Err(e) => {
                                eprintln!(
                                    "[{}] fetch encode error: {}",
                                    stringify!($fn_name),
                                    e
                                );
                                -1
                            }
                        }
                    }
                    None => {
                        unsafe {
                            *output_ptr = std::ptr::null_mut();
                            *output_len = 0;
                        }
                        1 // no more data
                    }
                }
            }

            #[unsafe(no_mangle)]
            pub extern "C" fn [<$fn_name _finish>](
                ctx: *mut std::ffi::c_void,
            ) -> i32 {
                unsafe { drop(Box::from_raw(ctx as *mut $processor_type)) };
                0
            }
        }
    };
}
