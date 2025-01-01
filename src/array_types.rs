use smallvec::Array;
pub const ORDER: usize = 2_usize.pow(6);
pub const MAX_KEYS_PER_NODE: usize = ORDER - 1; // number of search keys per node
pub const MIN_KEYS_PER_NODE: usize = ORDER / 2; // number of search keys per node
pub const KV_IDX_CENTER: usize = MAX_KEYS_PER_NODE / 2;
pub const MAX_CHILDREN_PER_NODE: usize = ORDER;

pub const KEY_TEMP_ARRAY_SIZE: usize = ORDER;
pub const VALUE_TEMP_ARRAY_SIZE: usize = ORDER;

pub const CHILD_TEMP_ARRAY_SIZE: usize = ORDER + 1;

macro_rules! define_array_types {
    ($name:ident, $size:expr) => {
        pub struct $name<T>(pub [T; $size]);

        unsafe impl<T> Array for $name<T> {
            type Item = T;
            fn size() -> usize {
                $size
            }
        }
    };
}

define_array_types!(KeyArray, MAX_KEYS_PER_NODE);
define_array_types!(ValueArray, MAX_KEYS_PER_NODE);
define_array_types!(ChildArray, MAX_CHILDREN_PER_NODE);
define_array_types!(KeyTempArray, KEY_TEMP_ARRAY_SIZE);
define_array_types!(ValueTempArray, VALUE_TEMP_ARRAY_SIZE);
define_array_types!(ChildTempArray, CHILD_TEMP_ARRAY_SIZE);
