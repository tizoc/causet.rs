//! `#[derive(Action)]`, `#[derive(Effect)]`, `#[derive(Event)]` and
//! `#[derive(Output)]`: each implements `Message` (`TYPE`, `kind()`, logging)
//! and the category's marker trait.
//!
//! ```text
//! #[derive(Debug, Action)]
//! #[causet(type = "downloader.action")]          // required: the type's name
//! enum DownloaderAction {
//!     Fetch(String),                             // kind "downloader.fetch"
//!     #[causet(kind = "downloader.got")]
//!     Completed { bytes: usize },                // kind overridden
//!     #[causet(nested)]
//!     Retry(RetryAction),                        // kind delegated to the inner message
//! }
//! ```
//!
//! Type-level options: `type` (required), `prefix` (the kinds' prefix; default
//! `type` without its last `.` segment), `kind` (a struct's kind; default
//! `type`), `crate` (the path to causet; default `::causet`), `log`. Variant
//! options: `kind`, `nested` (a variant with one unnamed field holding a
//! message), `log`.
//!
//! `log(level = "warn", fields(peer, n = count, display(addr)))` chooses what
//! logging records at start: the level (`trace` to `error`; default `debug`,
//! or the type's level for its variants) and named fields, by `Debug` unless
//! written `display(...)`. A `nested` variant delegates both to the inner
//! message.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as Tokens};
use quote::{ToTokens, quote};
use syn::meta::ParseNestedMeta;
use syn::{
    Attribute, Data, DeriveInput, Expr, Fields, Ident, LitStr, Path, Token, parse_macro_input,
    parse_quote,
};

/// Implements `Message` and `Action`.
///
/// Attributes: `#[causet(type = "...")]` (required: the name recordings
/// use), `prefix = "..."`, `kind = "..."`, `log(level = "...", fields(...))`
/// and `crate = "..."` on the type; `kind`, `nested` and `log` on a variant.
#[proc_macro_derive(Action, attributes(causet))]
pub fn derive_action(input: TokenStream) -> TokenStream {
    derive(input, "Action")
}

/// Implements `Message` and `Effect`; attributes as for `Action`.
#[proc_macro_derive(Effect, attributes(causet))]
pub fn derive_effect(input: TokenStream) -> TokenStream {
    derive(input, "Effect")
}

/// Implements `Message` and `Event`; attributes as for `Action`.
#[proc_macro_derive(Event, attributes(causet))]
pub fn derive_event(input: TokenStream) -> TokenStream {
    derive(input, "Event")
}

/// Implements `Message` and `Output`; attributes as for `Action`.
#[proc_macro_derive(Output, attributes(causet))]
pub fn derive_output(input: TokenStream) -> TokenStream {
    derive(input, "Output")
}

fn derive(input: TokenStream, category: &str) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand(&input, category)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[derive(Default)]
struct TypeOptions {
    ty: Option<String>,
    prefix: Option<String>,
    kind: Option<String>,
    krate: Option<Path>,
    log: LogOptions,
}

#[derive(Default)]
struct VariantOptions {
    kind: Option<String>,
    nested: bool,
    log: LogOptions,
}

#[derive(Default)]
struct LogOptions {
    /// A `LogLevel` variant.
    level: Option<Ident>,
    fields: Vec<LogField>,
}

struct LogField {
    name: String,
    binding: Ident,
    display: bool,
}

/// `log(level = "...", fields(...))`.
fn parse_log(meta: ParseNestedMeta<'_>, into: &mut LogOptions) -> syn::Result<()> {
    meta.parse_nested_meta(|inner| {
        if inner.path.is_ident("level") {
            let lit: LitStr = inner.value()?.parse()?;
            let level = match lit.value().as_str() {
                "trace" => "Trace",
                "debug" => "Debug",
                "info" => "Info",
                "warn" => "Warn",
                "error" => "Error",
                _ => {
                    return Err(syn::Error::new_spanned(
                        lit,
                        "expected `trace`, `debug`, `info`, `warn` or `error`",
                    ));
                }
            };
            into.level = Some(Ident::new(level, lit.span()));
        } else if inner.path.is_ident("fields") {
            inner.parse_nested_meta(|field| parse_field(field, &mut into.fields))?;
        } else {
            return Err(inner.error("expected `level` or `fields`"));
        }
        Ok(())
    })
}

/// `x`, `name = x`, `display(x)`, `name = display(x)` (or `debug(...)`).
fn parse_field(field: ParseNestedMeta<'_>, into: &mut Vec<LogField>) -> syn::Result<()> {
    let single = |path: &Path| {
        path.get_ident()
            .cloned()
            .ok_or_else(|| syn::Error::new_spanned(path, "expected a field name"))
    };
    if field.input.peek(Token![=]) {
        let name = single(&field.path)?.to_string();
        let value: Expr = field.value()?.parse()?;
        let (binding, display) = match &value {
            Expr::Path(path) => (single(&path.path)?, false),
            Expr::Call(call) if call.args.len() == 1 => {
                let Expr::Path(func) = &*call.func else {
                    return Err(syn::Error::new_spanned(
                        &value,
                        "expected `debug(field)` or `display(field)`",
                    ));
                };
                let display = match single(&func.path)?.to_string().as_str() {
                    "debug" => false,
                    "display" => true,
                    _ => {
                        return Err(syn::Error::new_spanned(
                            &value,
                            "expected `debug(field)` or `display(field)`",
                        ));
                    }
                };
                let Expr::Path(arg) = &call.args[0] else {
                    return Err(syn::Error::new_spanned(
                        &call.args[0],
                        "expected a field name",
                    ));
                };
                (single(&arg.path)?, display)
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    &value,
                    "expected a field name, `debug(field)` or `display(field)`",
                ));
            }
        };
        into.push(LogField {
            name,
            binding,
            display,
        });
    } else if field.input.peek(syn::token::Paren) {
        let display = match single(&field.path)?.to_string().as_str() {
            "debug" => false,
            "display" => true,
            _ => return Err(field.error("expected `debug(field)` or `display(field)`")),
        };
        field.parse_nested_meta(|inner| {
            let binding = single(&inner.path)?;
            into.push(LogField {
                name: binding.to_string(),
                binding,
                display,
            });
            Ok(())
        })?;
    } else {
        let binding = single(&field.path)?;
        into.push(LogField {
            name: binding.to_string(),
            binding,
            display: false,
        });
    }
    Ok(())
}

/// The visitor calls for `fields`, and the pattern bindings they need.
fn log_calls(fields: &[LogField], krate: &Path) -> (Vec<Ident>, Tokens) {
    let mut bindings: Vec<Ident> = Vec::new();
    let mut calls = Vec::new();
    for field in fields {
        if !bindings.contains(&field.binding) {
            bindings.push(field.binding.clone());
        }
        let (name, binding) = (&field.name, &field.binding);
        calls.push(if field.display {
            quote!(#krate::__derive::FieldVisitor::display(__causet_visitor, #name, #binding);)
        } else {
            quote!(#krate::__derive::FieldVisitor::debug(__causet_visitor, #name, #binding);)
        });
    }
    (bindings, quote!(#(#calls)*))
}

fn type_options(attrs: &[Attribute]) -> syn::Result<TypeOptions> {
    let mut options = TypeOptions::default();
    for attr in attrs.iter().filter(|a| a.path().is_ident("causet")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("type") {
                options.ty = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("prefix") {
                options.prefix = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("kind") {
                options.kind = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("crate") {
                options.krate = Some(meta.value()?.parse::<LitStr>()?.parse()?);
            } else if meta.path.is_ident("log") {
                parse_log(meta, &mut options.log)?;
            } else {
                return Err(meta.error("expected `type`, `prefix`, `kind`, `crate` or `log`"));
            }
            Ok(())
        })?;
    }
    Ok(options)
}

fn variant_options(attrs: &[Attribute]) -> syn::Result<VariantOptions> {
    let mut options = VariantOptions::default();
    for attr in attrs.iter().filter(|a| a.path().is_ident("causet")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("kind") {
                options.kind = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("nested") {
                options.nested = true;
            } else if meta.path.is_ident("log") {
                parse_log(meta, &mut options.log)?;
            } else {
                return Err(meta.error("expected `kind`, `nested` or `log`"));
            }
            Ok(())
        })?;
    }
    if options.nested && options.kind.is_some() {
        return Err(syn::Error::new(
            Span::call_site(),
            "a variant is either `nested` or has a `kind`, not both",
        ));
    }
    if options.nested && (options.log.level.is_some() || !options.log.fields.is_empty()) {
        return Err(syn::Error::new(
            Span::call_site(),
            "a `nested` variant logs as its inner message: no `log`",
        ));
    }
    Ok(options)
}

fn expand(input: &DeriveInput, category: &str) -> syn::Result<Tokens> {
    let options = type_options(&input.attrs)?;
    let ty = options.ty.clone().ok_or_else(|| {
        syn::Error::new_spanned(
            &input.ident,
            "missing `#[causet(type = \"...\")]`: the type's name",
        )
    })?;
    let krate = options
        .krate
        .clone()
        .unwrap_or_else(|| parse_quote!(::causet));
    let prefix = options.prefix.clone().unwrap_or_else(|| {
        ty.rsplit_once('.')
            .map_or_else(|| ty.clone(), |(prefix, _)| prefix.to_string())
    });

    let level_of = |level: &Option<Ident>| {
        level
            .as_ref()
            .or(options.log.level.as_ref())
            .map(|l| quote!(#krate::__derive::LogLevel::#l))
    };
    let debug_level = quote!(#krate::__derive::LogLevel::Debug);
    // `log_level` and `log_fields` bodies; `None` keeps the trait's default.
    let mut log_level = None;
    let mut log_fields = None;
    let named_only = |fields: &Fields, span: &dyn ToTokens| match fields {
        Fields::Named(_) | Fields::Unit => Ok(()),
        Fields::Unnamed(_) => Err(syn::Error::new_spanned(
            span,
            "log fields name named fields",
        )),
    };

    let kind = match &input.data {
        Data::Struct(data) => {
            log_level = level_of(&None);
            if !options.log.fields.is_empty() {
                named_only(&data.fields, &input.ident)?;
                let (bindings, calls) = log_calls(&options.log.fields, &krate);
                log_fields = Some(quote!(let Self { #(#bindings,)* .. } = self; #calls));
            }
            let kind = options.kind.clone().unwrap_or_else(|| ty.clone());
            quote!(#kind)
        }
        Data::Enum(data) => {
            if options.kind.is_some() {
                return Err(syn::Error::new_spanned(
                    &input.ident,
                    "a type-level `kind` applies to structs; give enum variants `kind`, or set `prefix`",
                ));
            }
            if !options.log.fields.is_empty() {
                return Err(syn::Error::new_spanned(
                    &input.ident,
                    "an enum's log fields go on its variants",
                ));
            }
            if data.variants.is_empty() {
                quote!(match *self {})
            } else {
                let mut arms = Vec::new();
                let (mut level_arms, mut field_arms) = (Vec::new(), Vec::new());
                let (mut any_level, mut any_fields) = (false, false);
                for variant in &data.variants {
                    let ident = &variant.ident;
                    let variant_options = variant_options(&variant.attrs)?;
                    if variant_options.nested {
                        match &variant.fields {
                            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                                arms.push(
                                    quote!(Self::#ident(inner) => #krate::Message::kind(inner)),
                                );
                                level_arms.push(quote!(Self::#ident(inner) => #krate::Message::log_level(inner)));
                                field_arms.push(
                                    quote!(Self::#ident(inner) => #krate::Message::log_fields(inner, __causet_visitor)),
                                );
                                (any_level, any_fields) = (true, true);
                            }
                            _ => {
                                return Err(syn::Error::new_spanned(
                                    variant,
                                    "a `nested` variant holds exactly one unnamed field: the inner message",
                                ));
                            }
                        }
                    } else {
                        let kind = variant_options.kind.unwrap_or_else(|| {
                            format!("{prefix}.{}", snake_case(&ident.to_string()))
                        });
                        let pattern = match &variant.fields {
                            Fields::Unit => quote!(Self::#ident),
                            Fields::Unnamed(_) => quote!(Self::#ident(..)),
                            Fields::Named(_) => quote!(Self::#ident { .. }),
                        };
                        arms.push(quote!(#pattern => #kind));
                        let level = level_of(&variant_options.log.level);
                        any_level |= level.is_some();
                        let level = level.unwrap_or_else(|| debug_level.clone());
                        level_arms.push(quote!(#pattern => #level));
                        if variant_options.log.fields.is_empty() {
                            field_arms.push(quote!(#pattern => {}));
                        } else {
                            named_only(&variant.fields, variant)?;
                            any_fields = true;
                            let (bindings, calls) = log_calls(&variant_options.log.fields, &krate);
                            field_arms
                                .push(quote!(Self::#ident { #(#bindings,)* .. } => { #calls }));
                        }
                    }
                }
                if any_level {
                    log_level = Some(quote!(match self { #(#level_arms,)* }));
                }
                if any_fields {
                    log_fields = Some(quote!(match self { #(#field_arms,)* }));
                }
                quote!(match self { #(#arms,)* })
            }
        }
        Data::Union(_) => {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "messages are structs or enums",
            ));
        }
    };

    let name = &input.ident;
    let category = Ident::new(category, Span::call_site());
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    let log_level =
        log_level.map(|body| quote!(fn log_level(&self) -> #krate::__derive::LogLevel { #body }));
    let log_fields = log_fields.map(|body| {
        quote! {
            #[allow(unused_variables)]
            fn log_fields(&self, __causet_visitor: &mut dyn #krate::__derive::FieldVisitor) { #body }
        }
    });
    Ok(quote! {
        impl #impl_generics #krate::Message for #name #type_generics #where_clause {
            const TYPE: &'static str = #ty;
            fn kind(&self) -> &'static str {
                #kind
            }
            #log_level
            #log_fields
        }
        impl #impl_generics #krate::#category for #name #type_generics #where_clause {}
    })
}

fn snake_case(name: &str) -> String {
    let mut out = String::new();
    for (i, c) in name.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}
