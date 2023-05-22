#![feature(proc_macro_diagnostic, proc_macro_span)]
#![allow(clippy::explicit_auto_deref)]

use std::convert::identity;
use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use proc_macro2::Span;
use quote::ToTokens;
use syn::{
    parse_macro_input, parse_quote, AttrStyle, Expr, ExprLit, Ident, ItemConst, Lit, Member, Meta,
    MetaNameValue, Path, Type,
};

#[proc_macro]
pub fn quote_to_str(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let lit = proc_macro::Literal::string(&*item.to_string());
    let tt = proc_macro::TokenTree::from(lit);
    [tt].into_iter().collect()
}

#[proc_macro_attribute]
pub fn operator_docgen(
    _attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let item = parse_macro_input!(item as ItemConst);
    if let Err(err) = operator_docgen_internal(&item) {
        eprint!("Failed to generate docs: {}", err);
    }
    item.into_token_stream().into()
}

/// Extracts doctest information and writes it to ~/book/docgen for each operator.
fn operator_docgen_internal(item: &ItemConst) -> Result<(), Box<dyn Error>> {
    assert_eq!(
        identity::<Type>(parse_quote!(OperatorConstraints)),
        *item.ty
    );

    let Expr::Struct(expr_struct) = &*item.expr else {
        panic!("Unexpected non-struct expression.");
    };
    assert_eq!(
        identity::<Path>(parse_quote!(OperatorConstraints)),
        expr_struct.path
    );

    let name_field = expr_struct
        .fields
        .iter()
        .find(|&field_value| identity::<Member>(parse_quote!(name)) == field_value.member)
        .expect("Expected `name` field not found.");
    let Expr::Lit(ExprLit { lit: Lit::Str(op_name), .. }) = &name_field.expr else {
        panic!("Unexpected non-literal or non-str `name` field value.")
    };
    let op_name = op_name.value();

    let docgen_path = PathBuf::from_iter([
        std::env!("CARGO_MANIFEST_DIR"),
        "../docs/docgen",
        &*format!("{}.md", op_name),
    ]);
    let mut docgen_write = BufWriter::new(File::create(docgen_path)?);
    writeln!(
        docgen_write,
        "<!-- GENERATED {:?} -->",
        Span::call_site()
            .unwrap()
            .source_file()
            .path()
            .to_string_lossy()
            .replace(std::path::MAIN_SEPARATOR, "/")
    )?;

    let mut in_hf_doctest = false;
    for attr in item.attrs.iter() {
        let AttrStyle::Outer = attr.style else { continue; };
        let Meta::NameValue(MetaNameValue { path, eq_token: _, value }) = &attr.meta else { continue; };
        let Some("doc") = path.get_ident().map(Ident::to_string).as_deref() else { continue; };
        let Expr::Lit(ExprLit { attrs: _, lit }) = value else { continue; };
        let Lit::Str(doc_lit_str) = lit else { continue; };
        // At this point we know we have a `#[doc = "..."]`.
        let doc_str = doc_lit_str.value();
        let doc_str = doc_str.strip_prefix(' ').unwrap_or(&*doc_str);
        if doc_str.trim_start().starts_with("```") {
            if in_hf_doctest {
                in_hf_doctest = false;
                writeln!(docgen_write, "{}", DOCTEST_HYDROFLOW_SUFFIX)?;
                // Output `doc_str` below.
            } else if doc_str.trim() == "```hydroflow" {
                in_hf_doctest = true;
                writeln!(docgen_write, "{}", DOCTEST_HYDROFLOW_PREFIX)?;
                continue;
            } else if doc_str.trim() == "```rustbook" {
                writeln!(docgen_write, "```rust")?;
                continue;
            }
        }
        writeln!(docgen_write, "{}", doc_str)?;
    }

    Ok(())
}

const DOCTEST_HYDROFLOW_PREFIX: &str = "\
```rust
# #[allow(unused_imports)] use hydroflow::{var_args, var_expr};
# #[allow(unused_imports)] use hydroflow::pusherator::Pusherator;
# let __rt = hydroflow::tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
# __rt.block_on(async {
# let mut __hf = hydroflow::hydroflow_syntax! {";
const DOCTEST_HYDROFLOW_SUFFIX: &str = "\
# };
# for _ in 0..100 {
#     hydroflow::tokio::task::yield_now().await;
#     if !__hf.run_tick() {
#         // No work done.
#         break;
#     }
# }
# })";
