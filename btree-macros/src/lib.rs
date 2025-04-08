extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

#[proc_macro_attribute]
pub fn qsbr_test(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let fn_block = &input_fn.block;
    let fn_attrs = &input_fn.attrs;
    let fn_sig = &input_fn.sig;

    let expanded = quote! {
        #(#fn_attrs)*
        #[test]
        #fn_sig
        {
            let _guard = qsbr::qsbr_reclaimer().guard();
            {
                #fn_block
            }
        }
    };

    TokenStream::from(expanded)
}
