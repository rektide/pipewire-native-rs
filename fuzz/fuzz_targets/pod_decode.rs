#![no_main]

use libfuzzer_sys::fuzz_target;
use pipewire_native_spa::param::{props::PropInfo, ParamType};
use pipewire_native_spa::pod::parser::Parser;
use pipewire_native_spa::pod::types::{Choice, Fd, Fraction, Id, Pointer, Rectangle};
use pipewire_native_spa::pod::{Pod, RawPod, RawPodOwned};

const MAX_INPUT_SIZE: usize = 64 * 1024;

fn decode<T: Pod>(data: &[u8]) {
    let _ = T::decode(data);
}

fn drain_raw_pods(parser: &mut Parser<'_>) {
    while parser.available() != 0 {
        if parser.pop_raw_pod().is_err() {
            break;
        }
    }
}

fn exercise_typed_decoders(data: &[u8]) {
    decode::<()>(data);
    decode::<bool>(data);
    decode::<Id<ParamType>>(data);
    decode::<i32>(data);
    decode::<i64>(data);
    decode::<f32>(data);
    decode::<f64>(data);
    decode::<String>(data);
    decode::<Vec<u8>>(data);
    decode::<Pointer>(data);
    decode::<Fd>(data);
    decode::<Rectangle>(data);
    decode::<Fraction>(data);
    decode::<Vec<i32>>(data);
    decode::<Choice<i32>>(data);
    decode::<Option<i32>>(data);
}

fn exercise_typed_parser(data: &[u8]) {
    let _ = Parser::new(data).pop_none();
    let _ = Parser::new(data).pop_bool();
    let _ = Parser::new(data).pop_id::<ParamType>();
    let _ = Parser::new(data).pop_int();
    let _ = Parser::new(data).pop_long();
    let _ = Parser::new(data).pop_float();
    let _ = Parser::new(data).pop_double();
    let _ = Parser::new(data).pop_string();
    let _ = Parser::new(data).pop_bytes();
    let _ = Parser::new(data).pop_pointer();
    let _ = Parser::new(data).pop_fd();
    let _ = Parser::new(data).pop_rectangle();
    let _ = Parser::new(data).pop_fraction();
    let _ = Parser::new(data).pop_array::<i32>();
    let _ = Parser::new(data).pop_choice::<i32>();
    let _ = Parser::new(data).pop_struct(|parser| {
        drain_raw_pods(parser);
        Ok(())
    });
    let _ = Parser::new(data).pop_object::<PropInfo, ParamType, _>(|parser, _| {
        while parser.available() != 0 {
            if parser.pop_property()?.is_none() {
                break;
            }
        }
        Ok(())
    });
}

fn exercise_raw_parser(data: &[u8]) {
    let _ = RawPod::wrap(data);
    let _ = RawPodOwned::wrap(data.to_vec());
    let _ = Parser::new(data).pop_raw_pod();
    let _ = Parser::new(data).pop_array_raw(|_, _| Ok(()));
    let _ = Parser::new(data).pop_choice_raw(|_, _| Ok(()));
    let _ = Parser::new(data).pop_object_raw::<u32, _>(|parser, _, _| {
        while parser.available() != 0 {
            if parser.pop_property()?.is_none() {
                break;
            }
        }
        Ok(())
    });
}

fuzz_target!(|input: &[u8]| {
    let data = &input[..input.len().min(MAX_INPUT_SIZE)];
    exercise_typed_decoders(data);
    exercise_typed_parser(data);
    exercise_raw_parser(data);
});
