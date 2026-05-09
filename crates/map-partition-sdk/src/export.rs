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
                array: *mut $crate::FFI_ArrowArray,
            ) -> i32 {
                let processor = unsafe { &mut *(ctx as *mut $processor_type) };
                let data_type = arrow::datatypes::DataType::Struct(
                    processor.schema().fields().clone(),
                );
                match unsafe {
                    $crate::import_batch_from_ffi(array, data_type)
                } {
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
                array: *mut $crate::FFI_ArrowArray,
            ) -> i32 {
                let processor = unsafe { &mut *(ctx as *mut $processor_type) };
                match processor.fetch() {
                    Some(batch) => {
                        match unsafe {
                            $crate::export_batch_to_ffi(batch, array)
                        } {
                            Ok(()) => 0, // more data may be available
                            Err(e) => {
                                eprintln!(
                                    "[{}] fetch error: {}",
                                    stringify!($fn_name),
                                    e
                                );
                                -1
                            }
                        }
                    }
                    None => 1, // no more data
                }
            }

            #[unsafe(no_mangle)]
            pub extern "C" fn [<$fn_name _finish>](
                ctx: *mut std::ffi::c_void,
            ) -> i32 {
                let processor = unsafe { &mut *(ctx as *mut $processor_type) };
                processor.finish();
                unsafe { drop(Box::from_raw(ctx as *mut $processor_type)) };
                0
            }
        }
    };
}
