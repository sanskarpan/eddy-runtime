//! Procedural macros for the `eddy` async runtime.
//!
//! - `#[eddy::main]` — wraps a `fn main` in an eddy runtime's `block_on`.
//! - `#[eddy::test]` — turns a test fn into a runtime-driven async test,
//!   with optional deterministic paused-time support.
//!
//! Attribute arguments are parsed with spans preserved so that errors point
//! at the exact user token; no builder code is emitted when parsing fails.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{parse_macro_input, Attribute, Error, ItemFn, Lit, Token};

enum Flavor {
    MultiThread,
    CurrentThread,
}

struct AttrArgs {
    flavor: Option<Flavor>,
    worker_threads: Option<usize>,
    start_paused: Option<bool>,
}

impl Parse for AttrArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut flavor = None;
        let mut worker_threads = None;
        let mut start_paused = None;

        while !input.is_empty() {
            let key: syn::Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            match key.to_string().as_str() {
                "flavor" => {
                    let lit: Lit = input.parse()?;
                    match lit {
                        Lit::Str(s) => {
                            let v = s.value();
                            flavor = Some(match v.as_str() {
                                "multi_thread" => Flavor::MultiThread,
                                "current_thread" => Flavor::CurrentThread,
                                _ => {
                                    return Err(Error::new(
                                        s.span(),
                                        "expected `flavor = \"current_thread\"` or `flavor = \"multi_thread\"`",
                                    ))
                                }
                            });
                        }
                        _ => {
                            return Err(Error::new(
                                lit.span(),
                                "expected a string literal for `flavor`",
                            ))
                        }
                    }
                }
                "worker_threads" => {
                    let lit: Lit = input.parse()?;
                    match lit {
                        Lit::Int(i) => match i.base10_parse::<usize>() {
                            Ok(n) if n > 0 => worker_threads = Some(n),
                            _ => {
                                return Err(Error::new(
                                    i.span(),
                                    "`worker_threads` must be a positive integer",
                                ))
                            }
                        },
                        _ => {
                            return Err(Error::new(
                                lit.span(),
                                "expected an integer literal for `worker_threads`",
                            ))
                        }
                    }
                }
                "start_paused" => {
                    let lit: Lit = input.parse()?;
                    match lit {
                        Lit::Bool(b) => start_paused = Some(b.value),
                        _ => {
                            return Err(Error::new(
                                lit.span(),
                                "expected a boolean literal for `start_paused`",
                            ))
                        }
                    }
                }
                other => {
                    return Err(Error::new(
                        key.span(),
                        format!("unknown attribute argument `{other}`"),
                    ))
                }
            }
            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
        }

        Ok(AttrArgs {
            flavor,
            worker_threads,
            start_paused,
        })
    }
}

impl AttrArgs {
    fn builder_expr(&self, default: Flavor) -> TokenStream2 {
        let flavor = match self.flavor {
            Some(Flavor::MultiThread) => quote! { eddy::Builder::new_multi_thread() },
            Some(Flavor::CurrentThread) => quote! { eddy::Builder::new_current_thread() },
            None => match default {
                Flavor::MultiThread => quote! { eddy::Builder::new_multi_thread() },
                Flavor::CurrentThread => quote! { eddy::Builder::new_current_thread() },
            },
        };
        match (self.worker_threads, self.start_paused) {
            (Some(n), _) => quote! { #flavor.worker_threads(#n) },
            _ => flavor,
        }
    }
}

fn validate_item(item: &ItemFn, kind: &str) -> syn::Result<()> {
    if !item.sig.generics.params.is_empty() {
        return Err(Error::new(
            item.sig.generics.span(),
            format!("#[{kind}] cannot be applied to a generic function"),
        ));
    }
    if item.sig.asyncness.is_some() {
        if let Some(async_token) = &item.sig.asyncness {
            return Err(Error::new(
                async_token.span(),
                format!(
                    "#[{kind}] cannot be applied to `async fn`; write a synchronous \
                     function and `.await` inside it"
                ),
            ));
        }
    }
    Ok(())
}

fn carry_attrs(item: &ItemFn) -> Vec<Attribute> {
    item.attrs
        .iter()
        .filter(|a| !a.path().is_ident("main") && !a.path().is_ident("test"))
        .cloned()
        .collect()
}

/// `#[eddy::main]` / `#[eddy::main(flavor = "current_thread", worker_threads = N)]`
#[proc_macro_attribute]
pub fn main(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as AttrArgs);
    let item = parse_macro_input!(input as ItemFn);

    if let Err(e) = validate_item(&item, "eddy::main") {
        return e.to_compile_error().into();
    }
    if item.sig.ident != "main" {
        return Error::new(
            item.sig.ident.span(),
            "#[eddy::main] must be applied to `fn main`",
        )
        .to_compile_error()
        .into();
    }

    let attrs = carry_attrs(&item);
    let stmts = &item.block.stmts;
    let builder = args.builder_expr(Flavor::MultiThread);

    let expanded = quote! {
        #(#attrs)*
        fn main() {
            let rt = #builder.build();
            rt.block_on(async {
                #(#stmts)*
            });
        }
    };
    expanded.into()
}

/// `#[eddy::test]` / `#[eddy::test(start_paused = true)]`
#[proc_macro_attribute]
pub fn test(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as AttrArgs);
    let item = parse_macro_input!(input as ItemFn);

    if let Err(e) = validate_item(&item, "eddy::test") {
        return e.to_compile_error().into();
    }
    if item.sig.ident == "main" {
        return Error::new(
            item.sig.ident.span(),
            "#[eddy::test] cannot be applied to `fn main`",
        )
        .to_compile_error()
        .into();
    }

    let attrs = carry_attrs(&item);
    let ident = &item.sig.ident;
    let stmts = &item.block.stmts;
    let builder = args.builder_expr(Flavor::CurrentThread);
    let paused = args.start_paused.unwrap_or(false);

    let expanded = if paused {
        quote! {
            #(#attrs)*
            #[test]
            fn #ident() {
                let rt = #builder.build();
                rt.block_on(async {
                    eddy::time::pause();
                    eddy::time::auto_advance(true);
                    #(#stmts)*
                });
            }
        }
    } else {
        quote! {
            #(#attrs)*
            #[test]
            fn #ident() {
                let rt = #builder.build();
                rt.block_on(async {
                    #(#stmts)*
                });
            }
        }
    };
    expanded.into()
}
